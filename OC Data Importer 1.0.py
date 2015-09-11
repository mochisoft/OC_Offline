#import wx
import psycopg2

import csv
#import psycopg2
import sys, httplib,logging
import datetime,time

import re
import xml.sax.handler
"""
@Author: Michael Ochieng
        Data Manager/ Programmer
        DNDi Africa Liason Office
        Nairobi, Kenya.
        updated: 11th Sep, 2015
        Pythons Script For Importing OpenClinica Data from a site study into the Central Database
        ------------------------------------------------------------------------------------------------
       Assumptions
       ------------------------------------------------------------------------------------------------
       1. The Site Database Must be a replica of the Central Database.
       2. The Site database backup dump must be plugged into the same server containing the central database
"""

def xml2obj(src):


    non_id_char = re.compile('[^_0-9a-zA-Z]')
    def _name_mangle(name):
        return non_id_char.sub('_', name)

    class DataNode(object):
        def __init__(self):
            self._attrs = {}    # XML attributes and child elements
            self.data = None    # child text data
        def __len__(self):
            # treat single element as a list of 1
            return 1
        def __getitem__(self, key):
            if isinstance(key, basestring):
                return self._attrs.get(key,None)
            else:
                return [self][key]
        def __contains__(self, name):
            return self._attrs.has_key(name)
        def __nonzero__(self):
            return bool(self._attrs or self.data)
        def __getattr__(self, name):
            if name.startswith('__'):
                # need to do this for Python special methods???
                raise AttributeError(name)
            return self._attrs.get(name,None)
        def _add_xml_attr(self, name, value):
            if name in self._attrs:
                # multiple attribute of the same name are represented by a list
                children = self._attrs[name]
                if not isinstance(children, list):
                    children = [children]
                    self._attrs[name] = children
                children.append(value)
            else:
                self._attrs[name] = value
        def __str__(self):
            return self.data or ''
        def __repr__(self):
            items = sorted(self._attrs.items())
            if self.data:
                items.append(('data', self.data))
            return u'{%s}' % ', '.join([u'%s:%s' % (k,repr(v)) for k,v in items])

    class TreeBuilder(xml.sax.handler.ContentHandler):
        def __init__(self):
            self.stack = []
            self.root = DataNode()
            self.current = self.root
            self.text_parts = []
        def startElement(self, name, attrs):
            self.stack.append((self.current, self.text_parts))
            self.current = DataNode()
            self.text_parts = []
            # xml attributes --> python attributes
            for k, v in attrs.items():
                self.current._add_xml_attr(_name_mangle(k), v)
        def endElement(self, name):
            text = ''.join(self.text_parts).strip()
            if text:
                self.current.data = text
            if self.current._attrs:
                obj = self.current
            else:
                # a text only node is simply represented by the string
                obj = text or ''
            self.current, self.text_parts = self.stack.pop()
            self.current._add_xml_attr(_name_mangle(name), obj)
        def characters(self, content):
            self.text_parts.append(content)

    builder = TreeBuilder()
    if isinstance(src,basestring):
        xml.sax.parseString(src, builder)
    else:
        xml.sax.parse(src, builder)
    return builder.root._attrs.values()[0]
## end of http://code.activestate.com/recipes/534109/ }}}




def sendSOAPMessage(soap_envelop,post,host,port):
    
    _post = post
    _host = host
    _port = port
    
    # now, we open an HTTP connection, set required headers, and send the SOAP envelope
    envlen = len(soap_envelop) 
    http_conn = httplib.HTTP(_host, _port) 
    http_conn.putrequest('POST', _post) 
    http_conn.putheader('Host', _host) 
    http_conn.putheader('Content-Type', 'text/xml; charset="utf-8"') 
    http_conn.putheader('Content-Length', str(envlen)) 
    http_conn.putheader('SOAPAction', '') 
    http_conn.endheaders() 
    http_conn.send(soap_envelop) 

    # fetch HTTP reply headers and the response
    (status_code, message, reply_headers) = http_conn.getreply() 
    response = http_conn.getfile().read() 


    return response
    
    

def getDBConnection(central_dbname,central_username,central_host,central_passwd,site_dbname,
                    site_username,site_host,site_passwd,tomcat_host,tomcat_port,oc_instance_name,
                    study_identifier,site_identifier,soap_username,soap_password):
    """
    study_identifier=study_identifier
    site_identifier=site_identifier
    soap_username=soap_username
    soap_password=soap_password
    
    """
    dt=datetime.datetime.now()
    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s, %(name)-12s, %(levelname)-8s, %(message)s,',
                    datefmt='%m-%d %H:%M',
                    filename='dataimport_'+str(dt.year)+'_'+str(dt.month)+'_'+str(dt.day)+'_'+str(dt.hour)+'_'+str(dt.minute)+'_'+str(dt.second)+'.csv',
                    filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    # Now, define a couple of other loggers which might represent areas in your
    # application:

    logger1 = logging.getLogger('Data_Import')
    logger2 = logging.getLogger('Data_Import2')

    logger1.info('STUDY TITLE')
    logger1.info('Site Data Import into Central OpenClinica Database')
    logger1.info('Data Import Report')
    logger1.info('Date:'+str(datetime.datetime.now()))
    logger1.info('Study Site Identifier:'+site_identifier)
    logger1.info('-----------------------------------------------------------------------------------')

    try:

        
        logging.info('Attempting database connection.................')
        """Connection to site database i.e the database to import data from"""
        conn = psycopg2.connect("dbname='"+site_dbname+"' user='"+site_username+"' host='"+site_host+"' password='"+site_passwd+"'")
        logging.info('Connected to the DB!')
        cur=conn.cursor()
        cur3=conn.cursor()
        cur4=conn.cursor()
        datacur=conn.cursor()
        datacur2=conn.cursor()
        datacur3=conn.cursor()
        datacur4=conn.cursor()
        datacur5=conn.cursor()
        datacur6=conn.cursor()
        
        """Conncetion to the Main Study Database i.e the database to import data to"""
        conn2 = psycopg2.connect("dbname='"+central_dbname+"' user='"+central_username+"' host='"+central_host+"' password='"+central_passwd+"'")
        cur2=conn2.cursor()   

        """
        1. We begin by importing the subject from OpenClinica subject table.
        First, select all the study_subjects from the site study database so the we can
        compare with study_subjects from the Main study database whether they are already
        in the system or not.
        """

        logger1.info("------------Importing Subject Data---------------------- ")
        cur.execute("select * from study_subject where study_id=(select study_id from study where unique_identifier='"+site_identifier+"');")
        study_subject_rows = cur.fetchall()
        the_subject_id=0
        
        if study_subject_rows:
            #p=ProgressBar(len(study_subject_rows))
            i=0
            for row in study_subject_rows:
                
                
                print row
                datacur.execute("select a.oc_oid,b.oc_oid,b.label from study a inner join study_subject b on(a.study_id=b.study_id) where b.oc_oid='"+str(row[11])+"'")
                data=datacur.fetchone()
                study_oid=str(data[0])
                subject_oid=str(data[1])
                subject_label=str(data[2])
                post='/'+oc_instance_name+'/ws/data/v1/dataWsdl.wsdl'

                logger1.info("--------------------------------------------------"+subject_label+"-----------------------------------------")

                datacur2.execute("select  b.oc_oid,a.study_event_id,b.name,a.sample_ordinal from study_event a inner join study_event_definition b on (a.study_event_definition_id=b.study_event_definition_id) where a.study_subject_id=(select study_subject_id from study_subject where label='"+row[1]+"')")
                data2=datacur2.fetchall()
                if data2:
                    for data2_row in data2:
                        study_event_oid=data2_row[0]
                        study_event_name=data2_row[2]
                        study_event_repeat_key=data2_row[3]
                                      
                        datacur3.execute("select b.oc_oid,a.event_crf_id from event_crf a  inner join crf_version b on (b.crf_version_id=a.crf_version_id) where a.study_event_id="+str(data2_row[1]))
                        data3=datacur3.fetchall()
                        if data3:
                            for data3_row in data3:
                                form_oid=data3_row[0]

                                data_envelop="""
                                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/data/v1" xmlns:bean="http://openclinica.org/ws/beans">
                                    <soapenv:Header>
                                          <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                                             <wsse:UsernameToken wsu:Id="UsernameToken-27777511" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                                                <wsse:Username>%s</wsse:Username>
                                                <wsse:Password type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">%s</wsse:Password>
                                             </wsse:UsernameToken>
                                          </wsse:Security>
                                    </soapenv:Header>
                                    <soapenv:Body>
                                        <v1:importRequest>
                                          <ODM>
                                            <ClinicalData StudyOID="%s" MetaDataVersionOID="v1.0.0">                                            
                                              <SubjectData SubjectKey="%s">
                                                 <StudyEventData StudyEventOID="%s" StudyEventRepeatKey="%s">
                                """
                                data_envelop=data_envelop % (soap_username,soap_password,study_oid,subject_oid,study_event_oid,study_event_repeat_key)



                                data_envelop+="""\t<FormData FormOID="%s">\n"""
                                data_envelop=data_envelop % (form_oid)
                                datacur4.execute("select d.oc_oid,a.event_crf_id,d.name,c.name from event_crf a inner join crf_version b on (a.crf_version_id=b.crf_version_id) inner join crf c on (b.crf_id=c.crf_id) inner join item_group d on (c.crf_id=d.crf_id) where event_crf_id="+str(data3_row[1]))
                                data4=datacur4.fetchall()
                                if data4:
                                    for data4_row in data4:
                                        itemgroup_oid=str(data4_row[0])
                                        event_crf_id=str(data4_row[1])
                                        itemgroup_name=str(data4_row[2])
                                        crf_name=str(data4_row[3])
                                        if itemgroup_name=="Ungrouped":
                                            data_envelop+="""\t\t<ItemGroupData ItemGroupOID="%s" TransactionType="Insert">\n"""
                                            data_envelop=data_envelop % (itemgroup_oid)
                                            
                                            datacur5.execute("""select b.oc_oid,a.value,b.units,c.oc_oid,a.ordinal,g.oc_oid from item_data a inner join item b on(a.item_id=b.item_id) left outer
                                                            join measurement_unit c on (b.units=c.name) inner join event_crf d on (a.event_crf_id=d.event_crf_id) inner join crf_version e on
                                                            (d.crf_version_id=e.crf_version_id) inner join crf f on (e.crf_id=f.crf_id) inner join item_group g on (f.crf_id=g.crf_id)  inner join
                                                            item_group_metadata h on(h.item_group_id=g.item_group_id and h.item_id=b.item_id) where a.event_crf_id="""+event_crf_id+""" and g.oc_oid='"""+itemgroup_oid+"""'
                                                            and a.value not in ('') ;""")
                                            data5=datacur5.fetchall()

                                            if data5:
                                                for data5_row in data5:
                                                    item_oid=str(data5_row[0])
                                                    item_value=str(data5_row[1])
                                                    item_unit=str(data5_row[2])
                                                    if item_unit=="":
                                                        item_value=item_value.replace('&','&amp;')
                                                        item_value=item_value.replace('%','&#37;')
                                                        #print item_value
                                                        data_envelop+="""\t\t\t<ItemData ItemOID="%s" Value="%s"/>\n"""
                                                        data_envelop=data_envelop % (item_oid,item_value)
                                                    else:
                                                        item_value=item_value.replace('&','&amp;')
                                                        item_value=item_value.replace('%','&#37;')
                                                        
                                                        item_unit_oid=str(data5_row[3])
                                                        data_envelop+="""\t\t\t<ItemData ItemOID="%s" Value="%s">\n"""
                                                        data_envelop+="""\t\t\t\t<MeasurementUnitRef MeasurementUnitOID="%s"/>\n"""
                                                        data_envelop+="""\t\t\t</ItemData>\n"""
                                                        data_envelop=data_envelop % (item_oid,item_value,item_unit_oid)
                                                    
                                            data_envelop+="""\t\t</ItemGroupData>\n"""
                                        else:
                                            datacur5.execute("""select distinct xyz.ordinal from (select   b.oc_oid,a.value,b.units,c.oc_oid,a.ordinal from item_data a inner join item b
                                                            on(a.item_id=b.item_id) left outer join measurement_unit c on (b.units=c.name) /**/ inner join event_crf d on (a.event_crf_id=d.event_crf_id)
                                                            inner join crf_version e on (d.crf_version_id=e.crf_version_id) inner join crf f on (e.crf_id=f.crf_id) inner join item_group g on
                                                            (f.crf_id=g.crf_id) /**/ where a.event_crf_id="""+event_crf_id+""" and g.oc_oid='"""+itemgroup_oid+"""' and a.value not in ('') order by ordinal) xyz;""")
                                            data5=datacur5.fetchall()
                                            if data5:
                                                for data5_row in data5:
                                                    item_group_repeat_key=str(data5_row[0])
                                                    data_envelop+="""\t\t<ItemGroupData  TransactionType="Insert" ItemGroupOID="%s" ItemGroupRepeatKey="%s">\n"""
                                                    data_envelop=data_envelop % (itemgroup_oid,item_group_repeat_key)
                                                    datacur6.execute("""select b.oc_oid,a.value,b.units,c.oc_oid,a.ordinal,g.oc_oid from item_data a inner join item b on(a.item_id=b.item_id) left outer
                                                                    join measurement_unit c on (b.units=c.name) inner join event_crf d on (a.event_crf_id=d.event_crf_id) inner join crf_version e on
                                                                    (d.crf_version_id=e.crf_version_id) inner join crf f on (e.crf_id=f.crf_id) inner join item_group g on (f.crf_id=g.crf_id)  inner join
                                                                    item_group_metadata h on(h.item_group_id=g.item_group_id and h.item_id=b.item_id) where a.event_crf_id="""+event_crf_id+""" and g.oc_oid='"""+itemgroup_oid+"""'
                                                                    and a.value not in ('') and a.ordinal="""+item_group_repeat_key+""";""")
                                                    data6=datacur6.fetchall()
                                                    if data6:
                                                        for data6_row in data6:
                                                            
                                                            item_oid=str(data6_row[0])
                                                            item_value=str(data6_row[1])
                                                            item_unit=str(data6_row[2])
                                                            if item_unit=="":
                                                                item_value=item_value.replace('&','&amp;')
                                                                item_value=item_value.replace('%','&#37;')
                                                                
                                                                data_envelop+="""\t\t\t<ItemData ItemOID="%s" Value="%s"/>\n"""
                                                                data_envelop=data_envelop % (item_oid,item_value)
                                                            else:
                                                                item_value=item_value.replace('&','&amp;')
                                                                item_value=item_value.replace('%','&#37;')
                                                                
                                                                item_unit_oid=str(data6_row[3])
                                                                data_envelop+="""\t\t\t<ItemData ItemOID="%s" Value="%s">\n"""
                                                                data_envelop+="""\t\t\t\t<MeasurementUnitRef MeasurementUnitOID="%s"/>\n"""
                                                                data_envelop+="""\t\t\t</ItemData>\n"""
                                                                data_envelop=data_envelop % (item_oid,item_value,item_unit_oid)

                                                    data_envelop+="""\t\t</ItemGroupData>\n""" 
                                            
                                data_envelop+="""\t</FormData>\n"""
                                data_envelop+="""</StudyEventData>
                                               </SubjectData>
                                            </ClinicalData>
                                         </ODM>
                                      </v1:importRequest>
                                   </soapenv:Body>
                                </soapenv:Envelope>"""

                                #print "-------------------------------------:"
                                #print data_envelop
                                #print "-------------------------------------:"

                                #print data_envelop
                
                                soap_response=sendSOAPMessage(data_envelop,post,tomcat_host,tomcat_port)
                                soap_response=soap_response.replace('SOAP-ENV:','')
                                soap_response_obj=xml2obj(soap_response)
                                body=soap_response_obj.body
                                #print str(soap_response)
                                soap_result=str(soap_response_obj.Body.importDataResponse.result)
                                error_msg=str(soap_response_obj.Body.importDataResponse.error)
                                import_log="%s,%s,%s,%s,%s"
                                import_log=import_log % (subject_label,study_event_name,crf_name,soap_result,'All Items Imported Successfully'if error_msg.find('None')!=-1 else error_msg )
                                logger1.info( import_log)
                                

                
                i+=1
                
                    
                   
        logger1.info('Clossing DB Connections.............')
        cur.close()
        cur3.close()
        cur4.close()
        datacur.close()
        datacur2.close()
        datacur3.close()
        datacur4.close()
        datacur5.close()
        datacur6.close()
        logger1.info('All DB Connections Closed!')
        logger1.info('-----------------------------END------------------------------------')
        #conn.close()
                

    except psycopg2.DatabaseError, e:
        the_error= 'Error %s ' % e
        print 'Error %s' % e
        logger1.info(the_error)
        #return null

"""
@CentralDatabaseName: refers to the Central OC Database Name
@SiteDatabaseName: Refers to the database name for the site database as restored on the main server
@Username: Database user assigned to both databases
@localhostOrServeIP: The server IP Address or localhost if the script is executed from the server
@YourPassword: Refers to the user password corresponding to the @Username
Assumption
----------
1. The Same database login role is used when creating the CentralDatabase and the Site Database.
2. The User who is running the import should be Authorized to run Web services from the users profile.
3. If running the site import, the user running the import should be added as a site user and authorized to run web services.

"""

getDBConnection("test_study","clinica","localhost","clinica","test_study_a",
                    "clinica","localhost","clinica","localhost","8080","test_study_ws",
                    "TEST_STUDY","site_a","root","sedb17d44a26dg0198e7d3gh735d921f3kfkdfd6f0fac6bag9ffb3cjh")
