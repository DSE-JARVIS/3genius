import http.client

import ssl
import json
from io import StringIO
import mimetypes

"""
"
" The client interact with Waston Kownledge Catalog using Waston data API(beta)
" docu: https://cloud.ibm.com/apidocs/watson-data-api#introduction
" docu: https://developer.ibm.com/api/view/watsondata-prod:watson-data:title-Watson_Data_API#Introduction
" docu: https://tools.ietf.org/html/rfc6902
"
"""

class AnalyticsEngineClient():
    
    def __init__(self, host, uid=None, pwd=None, token=None, verbose=True):
        """
        @param::token: authentication token in string
        @param::host: host url in string
        return catalog client instance
        """
        if host == None:
            raise Exception('The host url is required.') 
        
#         if instance_display_name == None:
#             raise Exception('Analytics Engine display name is required.') 
#         else:
#             self.instance_display_name = instance_display_name
            
        if uid == None and pwd==None and token==None:
            raise Exception('The uid/pwd and authentication token can not be empty at the same time.')
        
        self.host = host
        # retrieve auth token
        if token == None:
            self.__get_auth_token__(uid,pwd)
        else:
            self.token = token   
            
#         if self.token != None:
#             self.__get_jobs_auth_token__(self.token, self.instance_display_name)
#         else:
#             raise Exception('Something went wrong, during getting jobs auth token') 
        # debug info
        if verbose:
            print('Initialize Cloud Pak For Data: sucessfully!')
    
    
    
    def get_all_instances(self):
        """
        return all the analytics instance details
        """
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        return self.__jsonify__(response)
    
    def get_all_volumes(self):
        """
        return all the analytics instance details
        """
        method = '/zen-data/v2/serviceInstance?type=volumes'
        response = self.__GET__(method)
        return self.__jsonify__(response)
    
    def get_all_storage_class(self):
        """
        returns all the available storage classes in the cluster.
        """
        method = '/zen-data/v2/storageclasses'
        response = self.__GET__(method)
        try:
            result = json.loads(response)
            if "requestObj" in result and len(result["requestObj"]) >0:
                result = [val["metadata"]["name"] for val in result["requestObj"]]
                return self.__jsonify__(json.dumps(result))
            
        except:
            return self.__jsonify__(response)
        
        return self.__jsonify__(response)
    
    
    def get_instance_id(self, instance_display_name):
        """
        @param string::instance_display_name: display name on the AE instance
        returns instance id for the AE instance 
        """
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        id = None
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps({"id":id}))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name:
                    id = val["ID"]
                    return self.__jsonify__(json.dumps({"id":id}))
    
        return self.__jsonify__(json.dumps({"id":id}))
    

    def get_instance_details(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns instance id for the AE instance 
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    result = val
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    def get_spark_end_point(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns Spark jobs end point url
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {"spark_jobs_endpoint": None}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    if "$HOST" in val["CreateArguments"]["connection-info"]["Spark jobs endpoint"]:
                        result["spark_jobs_endpoint"] = val["CreateArguments"]["connection-info"]["Spark jobs endpoint"].replace("$HOST", self.host)
                    else:
                        result["spark_jobs_endpoint"] = val["CreateArguments"]["connection-info"]["Spark jobs endpoint"]
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    
    def get_history_server_end_point(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance spark jobs history server endpoint
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {"history_server_endpoint": None}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    
                    if "$HOST" in val["CreateArguments"]["connection-info"]["History server endpoint"]:
                        result["history_server_endpoint"] = val["CreateArguments"]["connection-info"]["History server endpoint"].replace("$HOST", self.host)
                    else:
                        result["history_server_endpoint"] = val["CreateArguments"]["connection-info"]["History server endpoint"]
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    
    def get_history_server_ui_end_point(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance spark jobs history UI server
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        method = '/zen-data/v2/serviceInstance?type=spark'
        response = self.__GET__(method)
        response_dict = json.loads(response)
        result = {"history_server_endpoint": None}
        if len(response_dict["requestObj"]) == 0:
            return self.__jsonify__(json.dumps(result))
        else:
            for val in response_dict["requestObj"]:
                if val["ServiceInstanceDisplayName"] == instance_display_name or val["ID"] == instance_id:
                    
                    if "$HOST" in val["CreateArguments"]["connection-info"]["Spark jobs endpoint"]:
                        result["history_server_ui_endpoint"] = val["CreateArguments"]["connection-info"]["View history server"].replace("$HOST", self.host)
                    else:
                        result["history_server_ui_endpoint"] = val["CreateArguments"]["connection-info"]["View history server"]
                    return self.__jsonify__(json.dumps(result))
        return self.__jsonify__(json.dumps(result))
    
    def start_history_server(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance jobs history server
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        
        history_url= self.get_history_server_end_point(instance_display_name, instance_id)
        history_url= history_url["history_server_endpoint"].replace(self.host, "")
        print(history_url)
        response = self.__POST__(history_url)
        return self.__jsonify__(json.dumps(response))
    
    def stop_history_server(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns AE instance jobs history server
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None, need atleast one.")
        
        history_url= self.get_history_server_end_point(instance_display_name, instance_id)
        history_url= history_url["history_server_endpoint"].replace(self.host, "")
        response = self.__DELETE__(history_url)
        return self.__jsonify__(json.dumps(response))
    
    def submit_word_count_job(self, instance_display_name=None, instance_id=None ):
        """
        This method is can be used to check if you can submit jobs to AE spark instance.
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns instance id for the AE instance 
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
            
        spark_jobs_endpoint = self.get_spark_end_point(instance_display_name, instance_id)
        spark_jobs_endpoint= spark_jobs_endpoint["spark_jobs_endpoint"].replace(self.host, "")
        self.job_token = self.__get_jobs_auth_token__(self.token, instance_display_name)
        headers = {
            'jwt-auth-user-payload': self.job_token,
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        payload = {"engine":{
                        "type":"spark"
                            },
                   "application_arguments":["/opt/ibm/spark/examples/src/main/resources/people.txt"],
                   "application": "/opt/ibm/spark/examples/src/main/python/wordcount.py"
                  }
        payload = json.dumps(payload)
        print(spark_jobs_endpoint)
        #self, method, payloads=None, headers=None
        response = self.__POST__(spark_jobs_endpoint,payloads=payload, headers=headers)
        return self.__jsonify__(json.dumps(response))
    
    def submit_word_count_job2(self, instance_display_name=None, instance_id=None ):
         return self.submit_job(instance_display_name, application_arguments=["/opt/ibm/spark/examples/src/main/resources/people.txt"], application="/opt/ibm/spark/examples/src/main/python/wordcount.py")
    
    def submit_job(self, instance_display_name, instance_id=None, env ={}, volumes=[], size={}, application_arguments = [], application_jar=None, main_class=None, application=None   ):
        """
        This method used to submit jobs to AE instance
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns instance id for the AE instance 
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
            
        spark_jobs_endpoint = self.get_spark_end_point(instance_display_name, instance_id)
        spark_jobs_endpoint= spark_jobs_endpoint["spark_jobs_endpoint"].replace(self.host, "")
        self.job_token = self.__get_jobs_auth_token__(self.token, instance_display_name)
        type = "spark"
        payload = {
            "engine": {
                "type": "spark"
            }
        }
        
        if env != {}:
            payload["engine"]["env"] = env
            
        if volumes != []:
            payload["volumes"] = volumes
        
        if size != {}:
            payload["size"] = size
        
        if application_arguments != None:
            payload["application_arguments"] = application_arguments
        if application_jar != None:
            payload["application_jar"] = application_jar
        if main_class != None:
            payload["main_class"] = main_class
        if application != None:
            payload["application"] = application
        
        
        
        headers = {
            'jwt-auth-user-payload': self.job_token,
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
#         payload = {"engine":{
#                         "type":"spark"
#                             },
#                    "application_arguments":["/opt/ibm/spark/examples/src/main/resources/people.txt"],
#                    "application": "/opt/ibm/spark/examples/src/main/python/wordcount.py"
#                   }
        payload = json.dumps(payload)
        print(spark_jobs_endpoint)
        #self, method, payloads=None, headers=None
        response = self.__POST__(spark_jobs_endpoint,payloads=payload, headers=headers)
        return self.__jsonify__(json.dumps(response))
    
    def get_all_jobs(self, instance_display_name=None, instance_id=None ):
        """
        @param string::instance_display_name: display name on the AE instance
        @param int::instance_id: Instance ID on the AE instance
        returns instance id for the AE instance 
        """
        
        if instance_display_name == None and instance_id ==None:
            raise Exception("Both instance_display_name and instance_id can't be None")
        
        spark_jobs_endpoint = self.get_spark_end_point(instance_display_name, instance_id)
        spark_jobs_endpoint= spark_jobs_endpoint["spark_jobs_endpoint"].replace(self.host, "")
#         spark_jobs_endpoint = spark_jobs_endpoint.lstrip("/")
        self.job_token = self.__get_jobs_auth_token__(self.token, instance_display_name)
        headers = {
            'jwt-auth-user-payload': self.job_token,
            'cache-control': 'no-cache',
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        response = self.__GET__(spark_jobs_endpoint, headers=headers)
        return self.__jsonify__(json.dumps(response))
        
        
    def create_instance(self, instance_display_name,pre_existing_owner=False,transient_fields= {},service_instance_version = "3.0.1",create_arguments={} ):
        """
        @param string::instance_display_name: display name for the instance
        @param bool::pre_existing_owner: Set pre existing owner
        @param string::service_instance_version: set service instance version
        @param dict::transient_fields: dictionay to set transient fields, default is {}
        @param bool::pre_existing_owner: Set pre existing owner
        @param dict::create_arguments: set arguments for the volumes sample dictionary
            {
                "metadata":{
                   "volumeName":"volume name- must be created before",
                   "storageClass": "",
                   "storageSize": ""
                },
                "serviceInstanceDescription": "Description"
             }
        returns AE instance jobs history server
        """
        
        service_instance_type = "spark"
        
        if instance_display_name == None or len(instance_display_name) == 0:
            raise Exception("Instance display name can't be blank.")
        
        sample_creat_arguments = """
            {
                "metadata":{
                   "volumeName":"volume name- must be created before",
                   "storageClass": "",
                   "storageSize": ""
                },
                "serviceInstanceDescription": "Description"
             }"""
        
        if len(create_arguments) == 0:
            raise Exception("create_arguments dictionay can't be empty. Follow the sample: \n {}".format(sample_creat_arguments))
            
        if "metadata" not in create_arguments:
            raise Exception("create_arguments dictionay must have meta data. Follow the sample: \n {}".format(sample_creat_arguments))
        
        if "resources" not in create_arguments:
            create_arguments["resources"] = {}
        
        if "serviceInstanceDescription" not in create_arguments:
            create_arguments["serviceInstanceDescription"] = volume_instance_display_name
        
        payload = {
            "createArguments": create_arguments,
            "preExistingOwner": pre_existing_owner,
            "serviceInstanceDisplayName": instance_display_name,
            "serviceInstanceType": service_instance_type,
            "serviceInstanceVersion": service_instance_version,
            "transientFields": transient_fields
        }
        
        payload = json.dumps(payload)
        print(payload)
        
        method = '/zen-data/v2/serviceInstance'
        response = self.__POST__(method, payload)
        return self.__jsonify__(json.dumps(response))
     
        
    
    def create_volume(self, volume_instance_display_name,pre_existing_owner=False,transient_fields= {},service_instance_version = "-",create_arguments={} ):
        """
        @param string::volume_instance_display_name: display name for the volume
        @param bool::pre_existing_owner: Set pre existing owner
        @param string::service_instance_version: set service instance version
        @param dict::transient_fields: dictionay to set transient fields, default is {}
        @param bool::pre_existing_owner: Set pre existing owner
        @param dict::create_arguments: set arguments for the volumes sample dictionary
            {
                "metadata": {
                    "storageClass": "ibmc-file-gold-gid",
                    "storageSize": "20Gi"
                },
                "resources": {},
                "serviceInstanceDescription": "volume 1"
            }
        returns AE instance jobs history server
        """
        
        service_instance_type = "volumes"
        
        if volume_instance_display_name == None or len(volume_instance_display_name) == 0:
            raise Exception("volume instance display name can't be blank.")
        
        sample_creat_arguments = """
            {
                "metadata": {
                    "storageClass": "ibmc-file-gold-gid",
                    "storageSize": "20Gi"
                },
                "resources": {},
                "serviceInstanceDescription": "volume 1"
            }"""
        
        if len(create_arguments) == 0:
            raise Exception("create_arguments dictionay can't be empty. Follow the sample: \n {}".format(sample_creat_arguments))
            
        if "metadata" not in create_arguments:
            raise Exception("create_arguments dictionay must have meta data. Follow the sample: \n {}".format(sample_creat_arguments))
        
        if "resources" not in create_arguments:
            create_arguments["resources"] = {}
        
        if "serviceInstanceDescription" not in create_arguments:
            create_arguments["serviceInstanceDescription"] = volume_instance_display_name
        
        payload = {
            "createArguments": create_arguments,
            "preExistingOwner": pre_existing_owner,
            "serviceInstanceDisplayName": volume_instance_display_name,
            "serviceInstanceType": service_instance_type,
            "serviceInstanceVersion": service_instance_version,
            "transientFields": transient_fields
        }
        
        payload = json.dumps(payload)
        print(payload)
        
        method = '/zen-data/v2/serviceInstance'
        response = self.__POST__(method, payload)
        return self.__jsonify__(json.dumps(response))
    
    def start_volume(self, volume_name):
        """
        @param string::volume_name: volume display name
        returns response from API
        """
        
        if volume_name == None:
            raise Exception("volume display name cannot be empty.")
        
        method= "/zen-data/v1/volumes/volume_services/{}".format(volume_name)
        payload = json.dumps({})
        response = self.__POST__(method, payloads=payload)
        return self.__jsonify__(json.dumps(response))
    
    def get_file_from_volume(self, volume_name, source_file , target_file_name, target_directory= None):
        """
        @param string::volume_name: volume display name
        @param string::source_file: source complete file path
        @param string::target_file_name: name of the file to be saved on colume
        @param string::target_directory: path with directory structure, where file to be saved
        returns response from API
        """
        
        if volume_name == None:
            raise Exception("volume display name cannot be empty.")
        
        if source_file == None:
            raise Exception("source_file param cannot be empty.")
        
        if target_file_name == None:
            raise Exception("target_file_name param cannot be empty.")
        
        if target_directory != None:
            target_directory =  target_directory.lstrip("/")#.rstrip("/")
            target_directory =  target_directory.split("/")
            target_directory = "%2F".join(target_directory)
            method = "/zen-volumes/{}/v1/volumes/files/{}{}".format(volume_name, target_directory, target_file_name)
        else:
            method = "/zen-volumes/{}/v1/volumes/files/{}".format(volume_name, target_file_name)
        print(method)
        response = self.__GET__(method)
#         print(response.decode("utf-8"))
        return self.__jsonify__(json.dumps(response))
    
    def add_file_to_volume(self, volume_name, source_file , target_file_name, target_directory= None):
        """
        @param string::volume_name: volume display name
        @param string::source_file: source complete file path
        @param string::target_file_name: name of the file to be saved on colume
        @param string::target_directory: path with directory structure, where file to be saved
        returns response from API
        """
        
        if volume_name == None:
            raise Exception("volume display name cannot be empty.")
        
        if source_file == None:
            raise Exception("source_file param cannot be empty.")
        
        if target_file_name == None:
            raise Exception("target_file_name param cannot be empty.")
            
        
        conn = http.client.HTTPSConnection(self.host)
        dataList = []
        boundary = 'wL36Yn8afVp8Ag7AmP8qZ0SA4n1v9T'
        dataList.append('--' + boundary)
        dataList.append('Content-Disposition: form-data; name=upFile; filename={0}'.format(target_file_name))

        fileType = mimetypes.guess_type(source_file)[0] or 'application/octet-stream'
        dataList.append('Content-Type: {}'.format(fileType))
        dataList.append('')

        with open(source_file) as f:
          dataList.append(f.read())
        dataList.append('--'+boundary+'--')
        dataList.append('')
        body = '\r\n'.join(dataList)
        payload = body
        headers = {
          'Authorization': 'Bearer {}'.format(self.token),
          'Content-type': 'multipart/form-data; boundary={}'.format(boundary)
        }
        
        if target_directory != None:
            target_directory =  target_directory.lstrip("/")#.rstrip("/")
            target_directory =  target_directory.split("/")
            target_directory = "%2F".join(target_directory)
            method = "/zen-volumes/{}/v1/volumes/files/{}{}".format(volume_name, target_directory, target_file_name)
        else:
            method = "/zen-volumes/{}/v1/volumes/files/{}".format(volume_name, target_file_name)
        
        conn.request("PUT", method, payload, headers)
        res = conn.getresponse()
        result = res.read()
        print(result.decode("utf-8"))
        
        try:
            result = json.loads(result)
        except:
            self.__jsonify__(json.dumps(result))
            
        if "_messageCode_" in result:
            if result["_messageCode_"] == "Success":
                result["file_path"] = method#"/zen-volumes/{}/v1/volumes/files/{}/{}".format(volume_name, target_directory, target_file_name)
            else:
                self.__jsonify__(json.dumps(result))
                
        
        return self.__jsonify__(json.dumps(result))
    
    
    
    """
    "
    " authenicate user by username and password and get the authentication token 
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#creating-an-iam-bearer-token
    "
    """
    def __get_auth_token__(self, uid, pwd, verbose=False):
        """
        @param::uid: username
        @param::pwd: password
        return authentication token
        """
        if uid == None or pwd == None:
            raise Exception('the username and password are both required.')
        
        
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
            'password':pwd,
            'username':uid
        }
        method = '/v1/preauth/validateAuth'
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        self.token = self.__jsonify__(data)['accessToken']
        return self.token
    
    def __get_jobs_auth_token__(self, token, display_name, verbose=False):
        """
        @param::token: token
        @param::AE instance display name: display_name
        return jobs authentication token
        """
        if token == None:
            raise Exception('Platform token is required.')
        
        
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
            'Authorization': 'Bearer {}'.format(token)
        }
        payload = json.dumps({"serviceInstanceDisplayname": display_name})
        
        method = "/zen-data/v2/serviceInstance/token"
        conn.request("POST", method, headers=headers, body= payload)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        self.job_token = self.__jsonify__(data)['AccessToken']
        return self.job_token
            
        
    
    def __GET__(self, method, headers=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        print("{}/{}".format(self.host, method))
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    def __GET_CUSTOM__(self, url, method, headers=None):
        """
        @param string:: method: full API url
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              url,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    def __DELETE__(self, method, headers=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        
        conn.request("DELETE", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    
    def __POST__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
        print("{}/{}".format(self.host, method))
        conn.request("POST", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    def __PUT__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PUT", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    def __PATCH__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PATCH", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
        
    
    def __jsonify__(self, dumps):
        """
        @param::dumps: json dumps in string
        return json object
        """
        dumps_io = StringIO(dumps)
        return json.load(dumps_io)




#         ┌─┐       ┌─┐
#      ┌──┘ ┴───────┘ ┴──┐
#      │                 │
#      │       ───       │
#      │  ─┬┘       └┬─  │
#      │                 │
#      │       ─┴─       │
#      │                 │
#      └───┐         ┌───┘
#          │         │
#          │         │
#          │         │
#          │         └──────────────┐
#          │                        │
#          │                        ├─┐
#          │                        ┌─┘
#          │                        │
#          └─┐  ┐  ┌───────┬──┐  ┌──┘
#            │ ─┤ ─┤       │ ─┤ ─┤
#            └──┴──┘       └──┴──┘
#                 BLESSING FROM 
#           THE BUG-FREE MIGHTY BEAST