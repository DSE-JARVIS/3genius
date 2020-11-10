"""
"
" test watson service transformer class
"
"""
import os
import pytest
import json
from unittest import mock
from ibmaemagic.sdk.analytic_engine_client import AnalyticEngineClient
from uuid import uuid4
import http

# from CP4DMAGIC.src.ibmaemagic.sdk.analytic_engine_client import AnalyticEngineClient


@pytest.fixture(scope='function')
def mock_client(request):
    # define the mock of analytic engine client
    host = 'https://www.foo.com'
    token = 'fake_auth_token'
    client = AnalyticEngineClient(host, token=token)
    return client



class TestAnalyticEngineClient():

    def test_init_with_valid_param(self):
        # arrange
        host = 'https://www.foo.com'
        param_list = [{'token':'fake_auth_token', 'uid':None, 'pwd':None, 'env':None},
                      {'token':None, 'uid':'foo', 'pwd':'bar', 'env':None},
                      {'token':None, 'uid':None, 'pwd':None, 'env':'env_fake_token'}]
        token_list = ['fake_auth_token', 'ibm_auth_token','env_fake_token']
        for i,param in enumerate(param_list):
            token, uid, pwd, env = param['token'], param['uid'], param['pwd'], param['env']

            if env: # set enviroment variable
                os.environ['USER_ACCESS_TOKEN'] = env

            with mock.patch('http.client.HTTPSConnection') as mock_conn: # mock http client
                ibm_auth_response = json.dumps({'accessToken':'ibm_auth_token'})
                mock_conn().getresponse().read().decode= mock.MagicMock(return_value=ibm_auth_response)
                # act
                client = AnalyticEngineClient(host, token=token, uid=uid, pwd=pwd, verbose=False)
                # assert
                assert client.token == token_list[i]
                assert client.host == host

            if 'USER_ACCESS_TOKEN' in os.environ:
                del os.environ['USER_ACCESS_TOKEN'] 

    def test_init_with_invalid_param(self):
        # arange
        param_list = [{'host':'https://www.foobar.com', 'token':None},
                      {'host': None, 'token': 'fake_auth_token'},
                      {'host': None, 'token': None}]

        # act & assert
        for param in param_list:
            host, token = param['host'], param['token']
            with pytest.raises(Exception):
                AnalyticEngineClient(host, token=token)

    def test_get_all_instances(self, mock_client):
        # arrange
        json_message = json.dumps({'_messageCode_': 'success',"requestObj":["foo1","foo2","foo3"]})
        mock_client.__GET__ = mock.MagicMock(return_value=json_message)
        # act
        result = mock_client.get_all_instances()
        # assert
        assert isinstance(result, dict)
        assert result["_messageCode_"] == 'success'
        assert isinstance(result["requestObj"], list)
        assert len(result["requestObj"]) == 3
        assert result["requestObj"][0] == 'foo1'

    def test_get_all_instances_with_error(self, mock_client):
        # arrange
        mock_client.__GET__ = mock.MagicMock(side_effect=Exception('> Get request error.'))
        # act & assert
        with pytest.raises(Exception):
            mock_client.get_all_instances()
        mock_client.__GET__.assert_called_once()

    def test_get_all_volumes(self, mock_client):
        json_message = json.dumps({'_messageCode_': 'success',"requestObj":["foo1","foo2","foo3"]})
        mock_client.__GET__ = mock.MagicMock(return_value=json_message)
        # act
        result = mock_client.get_all_volumes()
        # assert
        assert isinstance(result, dict)
        assert result["_messageCode_"] == 'success'
        assert isinstance(result["requestObj"], list)
        assert len(result["requestObj"]) == 3
        assert result["requestObj"][0] == 'foo1'

    def test_get_all_storage_class(self, mock_client):
        json_message = json.dumps({'_messageCode_': 'success', "requestObj":[
            {"metadata":{"name": "default"}},
            {"metadata":{"name": "ibmc-block-bronze"}},
            {"metadata":{"name": "ibmc-block-custom"}}
        ]})
        mock_client.__GET__ = mock.MagicMock(return_value=json_message)
        # act
        result = mock_client.get_all_storage_class()
        # assert
        assert isinstance(result, list)
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0] == 'default'
        assert result[1] == 'ibmc-block-bronze'
        assert result[2] == 'ibmc-block-custom'

    def test_get_all_storage_class_with_error(self, mock_client):
        json_message = json.dumps({'_messageCode_': 'success', "requestObj":[
            {"metadata_new":{"name_new": "default"}},
            {"metadata_new":{"name_new": "ibmc-block-bronze"}},
            {"metadata_new":{"name_new": "ibmc-block-custom"}}
        ]})
        mock_client.__GET__ = mock.MagicMock(return_value=json_message)
        # act
        with pytest.raises(Exception):
            result = mock_client.get_all_storage_class()
        # assert

    def test_create_volume(self, mock_client):
        with pytest.raises(TypeError):
            mock_client.create_volume()

        #creating volume - without passing create_arguments
        with pytest.raises(Exception):  
            result = mock_client.create_volume("VOLUME_NAME")

        #creating volume - with empty create_arguments
        with pytest.raises(Exception):  
            result = mock_client.create_volume("VOLUME_NAME", create_arguments={})

        #creating volume - with create_arguments
        create_arguments = {
            "metadata": {
                "storageClass": "default",
                "storageSize": "1Gi"
            },
            "resources": {},
            "serviceInstanceDescription": "Volume created from unit test cases"
        }
        fake_json = {"_messageCode_":"200","id":"1602689675351","message":"Started provisioning the instance"}
        mock_client.__POST__ = mock.MagicMock(return_value=fake_json)
        result = mock_client.create_volume("VOLUME_NAME", create_arguments=create_arguments)
        assert isinstance(result, dict)
        assert result["_messageCode_"] == "200"
        assert result["id"] == "1602689675351"
        assert result["message"] == "Started provisioning the instance"

    def test_start_volume(self, mock_client):
        #start volume
        fake_json = {"_messageCode_":"200"}
        mock_client.__POST__ =mock.MagicMock(return_value=fake_json)

        with pytest.raises(Exception):
            result = mock_client.start_volume()    

        result = mock_client.start_volume("VOLUME_NAME")
        assert isinstance(result, dict)
        assert result["_messageCode_"]=="200"
    
    def test_volume_status(self, mock_client):
        
        with pytest.raises(Exception):
            result = mock_client.get_volume_status()
        
        fake_json = json.dumps({"requestObj":[]})
        mock_client.__GET__ =mock.MagicMock(return_value=fake_json)
        with pytest.raises(Exception):
            result = mock_client.get_volume_status()

        fake_json = json.dumps({"requestObj":[{"ID": "1234", "ServiceInstanceDisplayName": "VOLUME_NAME", "ProvisionStatus": "PROVISIONED"}]})
        mock_client.__GET__ =mock.MagicMock(return_value=fake_json)
        result = mock_client.get_volume_status(volume_name="VOLUME_NAME")
        assert result["status"] == "PROVISIONED"
    
    def test_add_file_to_volume(self, mock_client):

        with pytest.raises(TypeError):  
            result = mock_client.add_file_to_volume()
        
        with pytest.raises(TypeError):  
            result = mock_client.add_file_to_volume(volume_name="test")

        with pytest.raises(TypeError):  
            result = mock_client.add_file_to_volume(volume_name="test", source_file = "test")

        with mock.patch.object(http.client, 'HTTPSConnection', return_value=None):
                
            with pytest.raises(FileNotFoundError):  
                result = mock_client.add_file_to_volume(volume_name="test", source_file = str(uuid4()), target_file_name= "test1")
        #need to add few more
    
    def test_create_ae_instance(self, mock_client):

        with pytest.raises(TypeError):
            result = mock_client.create_instance()

        with pytest.raises(Exception):  
            result = mock_client.create_instance("some-random-name")

        with pytest.raises(Exception):  
            result = mock_client.create_instance("INSTANCE_NAME", create_arguments={})

        create_arguments_instance = {
            "metadata":{
            "volumeName": "unit-test-instance-volume-"+str(uuid4()),
            "storageClass": "",
            "storageSize": ""
            },
            "serviceInstanceDescription": "Instance volume created for unit test"
        }    

        fake_json = {"_messageCode_":"200"}
        mock_client.__POST__ =mock.MagicMock(return_value=fake_json)
        result = mock_client.create_instance("INSTANCE_NAME", create_arguments=create_arguments_instance)
        assert isinstance(result, dict)
        assert result["_messageCode_"] == "200"

    # def test_submit_word_count(self):
    #     fake_json = fj.submit_job_json
    #     with patch('ae_client.AnalyticsEngineClient.submit_word_count_job') as mock_get:
    #         mock_get.return_value= fake_json
    #         result = analytics_engine.submit_word_count_job(INSTANCE_NAME)
    #         self.assertIsInstance(result, dict)
    #         self.assertEqual(result["id"], fake_json["id"])
    #         self.assertEqual(result["job_state"], fake_json["job_state"])