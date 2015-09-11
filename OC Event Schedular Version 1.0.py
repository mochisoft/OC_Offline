import csv
import psycopg2
import sys, httplib,logging
import re
import xml.sax.handler
"""
@Author: Michael Ochieng
        Data Manager/ Programmer
        DNDi Africa Liason Office
        Nairobi, Kenya.
        updated: 11th Sep, 2015
        Pythons Script For Bulk Scheduling of OpenClinica Events in the Central Database
        - In preparation of Bulk Import from a site database.
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

def __getDBConnection(central_dbname,site_dbname,username,host,passwd,oc_ws_instance_name):
    try:
        ########set the following parameters with correct values
        #e.g oc_ws_url='http://localhost:8080/'+oc_ws_instance_name
        oc_ws_url=''
        # e.g post='/'+oc_ws_instance_name+'/ws/studySubject/v1/studySubjectWsdl.wsdl'
        post=''
        #e.g server_host='localhost'
        server_host=''
        #e.g server_port='8080'
        server_port=''
        #e.g oc_username='root'
        oc_username=''        
        #e.g oc_passwd='tfr2fgeb1744a26d0198e7d37353dd6f0ac6ba9b3cgfgfhgjjkj'
        oc_passwd=''
        #########End of parameter settings

        """Connection to site database i.e the database to import data from"""
        site_conn = psycopg2.connect("dbname='"+site_dbname+"' user='"+username+"' host='"+host+"' password='"+passwd+"'")
        cur=site_conn.cursor()
        cur3=site_conn.cursor()
        
        """Conncetion to the Main Study Database i.e the database to import data to"""
        centraldb_conn = psycopg2.connect("dbname='"+central_dbname+"' user='"+username+"' host='"+host+"' password='"+passwd+"'")
        cur2=centraldb_conn.cursor()   

        """
        1. We begin by importing the subject from OpenClinica subject table.
        First, select all the study_subjects from the site study database so the we can
        compare with study_subjects from the Main study database whether they are already
        in the system or not.
        """

        print "------------Importing Subject and Study_Subject data---------------------- "
        cur.execute("""select * from study_subject;""")
        study_subject_rows = cur.fetchall()
        if study_subject_rows:
            for study_subject in study_subject_rows:
                cur2.execute("""select * from study_subject where label='"""+study_subject[1]+"""'""")
                site_study_subject_rows=cur2.fetchall()

                """
                Meaning that if the subject with the label passed in the query above
                is not present, then go ahead and add the subject in the main database.
                """
                if len(site_study_subject_rows)==0:
                    
                    cur3.execute("""select ss.label,ss.secondary_label,ss.enrollment_date,s.unique_identifier,s.gender,s.date_of_birth,'' as year_of_birth,
                        (select unique_identifier from study where study_id=(select parent_study_id from study where study_id=st.study_id)) as study_identifier,st.unique_identifier as site_identifier from study_subject ss inner join subject s on (ss.subject_id=s.subject_id)
                         inner join study st on (ss.study_id=st.study_id) where ss.label='"""+study_subject[1]+"""';""")                    
                    ss_event_rows=cur3.fetchall()
                    
                    if ss_event_rows:
                        for ss_event_row in ss_event_rows:

                            data_envelop="""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/studySubject/v1" xmlns:bean="http://openclinica.org/ws/beans">
                                    <soapenv:Header>
                                          <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                                             <wsse:UsernameToken wsu:Id="UsernameToken-27777511" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                                                <wsse:Username>%s</wsse:Username>
                                                <wsse:Password type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">%s</wsse:Password>
                                             </wsse:UsernameToken>
                                          </wsse:Security>
                                    </soapenv:Header>
                                     <soapenv:Body>
                                           <v1:createRequest>
                                             <!--1 or more repetitions:-->
                                                 <v1:studySubject>
                                                     <bean:label>%s</bean:label>"""
                            data_envelop=data_envelop % (oc_username,oc_passwd,ss_event_row[0])
                            
                            #Check if SecondaryLabel is used
                            if ss_event_row[1]:

                                data_envelop+=""""<!--Optional:-->
                                                  <bean:secondaryLabel>%s</bean:secondaryLabel>"""
                                data_envelop=data_envelop % (ss_event_row[1])
                            data_envelop+="""<bean:enrollmentDate>%s</bean:enrollmentDate>
                                                <bean:subject>"""
                            data_envelop=data_envelop % (ss_event_row[2])
                            if ss_event_row[3]:
                                data_envelop+="""<!--Optional:-->
                                                             <bean:uniqueIdentifier>%s</bean:uniqueIdentifier> """
                                data_envelop=data_envelop % (ss_event_row[3])
                            if ss_event_row[4]:
                                data_envelop+="""<!--Optional:-->
                                                <bean:gender>%s</bean:gender> """
                                data_envelop=data_envelop % (ss_event_row[4])
                            if ss_event_row[5]:
                                data_envelop+="""<!--Optional:-->                                                             
                                                             <!--You have a CHOICE of the next 2 items at this level-->
                                                             <bean:dateOfBirth>%s</bean:dateOfBirth>"""
                                data_envelop=data_envelop % (ss_event_row[5])
                                
                            if ss_event_row[6]:
                                data_envelop+="""<bean:yearOfBirth>%s</bean:yearOfBirth> """
                                data_envelop=data_envelop % (ss_event_row[6])
                            data_envelop+="""</bean:subject>
                                                    <bean:studyRef>
                                                         <bean:identifier>%s</bean:identifier>"""
                            data_envelop=data_envelop % (ss_event_row[7])

                            if ss_event_row[8]:
                                data_envelop+="""<!--Optional:-->
                                                <bean:siteRef>
                                                             <bean:identifier>%s</bean:identifier>
                                                </bean:siteRef>"""
                                data_envelop=data_envelop % (ss_event_row[8])

                            data_envelop+="""
                                
                                                     </bean:studyRef>
                                                 </v1:studySubject>
                                             </v1:createRequest>
                                        </soapenv:Body>
                                    </soapenv:Envelope>
                            """
                            print data_envelop
                            soap_response=sendSOAPMessage(data_envelop,post,server_host,server_port)
                            soap_response=soap_response.replace('SOAP-ENV:','')
                            
                            print soap_response
                            soap_response_obj=xml2obj(soap_response)
                            body=soap_response_obj.body
    
                            soap_result=str(soap_response_obj.Body.createResponse.result)



        cur2.execute("""select study_subject_id,label from study_subject;""")
        ss_rows=cur2.fetchall()

        if ss_rows:
            for ss_row in ss_rows:
                print "------------Importing Study_Event data for ----------------------Subject ID: ", ss_row[1]
                cur.execute("""select ss.label,(select unique_identifier from study where study_id=st.parent_study_id) as study_identifier,
                                      st.unique_identifier as site_identifier,(select oc_oid from study_event_definition where study_event_definition_id=se.study_event_definition_id) as event_def_oid,
                                      se.location,se.date_start::date as date_start, to_char(se.date_start::time,'HH:MM') as time_start,se.date_end::date as date_end,to_char(se.date_end::time,'HH:MM') as time_end,se.study_event_definition_id,se.sample_ordinal 
                                       from study_subject ss inner join study st on (ss.study_id=st.study_id) inner join study_event se on (ss.study_subject_id=se.study_subject_id)
                                       where label='"""+str(ss_row[1])+"""';""")
                site_study_event_rows=cur.fetchall()
                if site_study_event_rows:

                    
                    for i in site_study_event_rows:

                        selEventStr="""select ss.label,(select unique_identifier from study where study_id=st.parent_study_id) as study_identifier,
                                      st.unique_identifier as site_identifier,(select oc_oid from study_event_definition where study_event_definition_id=se.study_event_definition_id) as event_def_oid,
                                      se.location,se.date_start::date as date_start, to_char(se.date_start::time,'HH:MM') as time_start,se.date_end::date as date_end,to_char(se.date_end::time,'') as time_end
                                       from study_subject ss inner join study st on (ss.study_id=st.study_id) inner join study_event se on (ss.study_subject_id=se.study_subject_id)
                                       where ss.label='%s' and se.study_event_definition_id=%s and se.sample_ordinal=%s;"""
                        selEventStr=selEventStr %(str(ss_row[1]),str(i[9]),str(i[10]))

                        cur2.execute(selEventStr)
                        maindb_se_rows=cur2.fetchall()
                       
                        if len(maindb_se_rows)==0:

                            data_envelop="""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/event/v1" xmlns:bean="http://openclinica.org/ws/beans">
                                    <soapenv:Header>
                                          <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                                             <wsse:UsernameToken wsu:Id="UsernameToken-27777511" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                                                <wsse:Username>root</wsse:Username>
                                                <wsse:Password type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">eb1744a26d0198e7d3735d9213dd6f0ac6ba9b3c</wsse:Password>
                                             </wsse:UsernameToken>
                                          </wsse:Security>
                                    </soapenv:Header>
                                       <soapenv:Body>
                                          <v1:scheduleRequest>
                                              <!--1 or more repetitions:-->
                                                 <v1:event>
                                                    <bean:studySubjectRef>
                                                        <bean:label>%s</bean:label>
                                                    </bean:studySubjectRef>
                                                    <bean:studyRef>
                                                    <bean:identifier>%s</bean:identifier>


                            """
                            data_envelop=data_envelop % (i[0],i[1])
                            
                            #Dynamically Build different parts of the SOAP envelop based on what is available from the site data
                            if i[2]:

                                data_envelop+="""<!--Optional:-->
                                                       <bean:siteRef>
                                                              <bean:identifier>%s</bean:identifier>
                                                       </bean:siteRef>
"""
                                data_envelop=data_envelop % (i[2])
                            data_envelop+="""</bean:studyRef>
                                            <bean:eventDefinitionOID>%s</bean:eventDefinitionOID>"""
                            data_envelop=data_envelop % (i[3])
                            if i[4]:
                                data_envelop+="""<!--Optional:-->
                                            <bean:location>%s</bean:location>
"""
                                data_envelop=data_envelop % (i[4])
                            if i[5]:
                                data_envelop+="""<bean:startDate>%s</bean:startDate>
"""
                                data_envelop=data_envelop % (i[5])
                            if i[6]:
                                data_envelop+="""<!--Optional:-->
                                                <bean:startTime>%s</bean:startTime>
"""
                                data_envelop=data_envelop % (i[6])
                                
                            if i[7]:
                                data_envelop+="""<!--Optional:-->
                                                <bean:endDate>%s</bean:endDate>
"""
                                data_envelop=data_envelop % (i[7])
                            if i[8]:
                                data_envelop+="""<!--Optional:-->
                                                <bean:endTime>%s</bean:endTime>
"""
                                data_envelop=data_envelop % (i[8])

                            data_envelop+="""</v1:event>
                                      </v1:scheduleRequest>
                                   </soapenv:Body>
                            </soapenv:Envelope>"""
                            
                            print data_envelop
                            soap_response=sendSOAPMessage(data_envelop,post,server_host,server_port)
                            soap_response=soap_response.replace('SOAP-ENV:','')
                            
                            print soap_response
                            soap_response_obj=xml2obj(soap_response)
                            body=soap_response_obj.body
    
                            soap_result=str(soap_response_obj.Body.scheduleResponse.result)
                            

        cur.close()
        site_conn.close()
        centraldb_conn.close()
                

    except psycopg2.DatabaseError, e:
        print 'Error %s' % e    
        

"""
@CentralDatabaseName: refers to the Central OC Database Name
@SiteDatabaseName: Refers to the database name for the site database as restored on the main server
@Username: Database user assigned to both databases
@localhostOrServerIP: The server IP Address or localhost if the script is executed from the server
@YourPassword: Refers to the user password corresponding to the db @Username
@OCWebServicesInstanceURL: Refers to the user password corresponding to the db @Username
Assumption
----------
1. The site database is restored onto the same server on which the central database resides in order
to reduce the number of parameters passed as argumenent here

"""

__getDBConnection('test_study','test_study_a','clinica','localhost','clinica','test_study_ws')

