from unittest.mock import patch, Mock

import pytest

from celery import states
from celery.backends.gcs import GCSBackend
from celery.exceptions import ImproperlyConfigured


MODULE_TO_MOCK = "celery.backends.gcs"


class test_GCSBackend:
    def test_missing_bucket(self):
        self.app.conf.result_backend = 'gs://'

        with pytest.raises(ImproperlyConfigured, match='Missing bucket name'):
            GCSBackend(app=self.app)

    def test_missing_project(self):
        self.app.conf.result_backend = 'gs://'
        self.app.conf.gcs_bucket = 'bucket'

        with pytest.raises(ImproperlyConfigured, match='Missing project'):
            GCSBackend(app=self.app)

    @pytest.fixture(params=['', 'my_folder/'])
    def base_path(self, request):
        return request.param

    @patch(MODULE_TO_MOCK + ".storage.bucket.Blob")
    def test_get(self, mock_blob_factory, base_path):
        self.app.conf.result_backend = 'gs://'
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'
        self.app.conf.gcs_base_path = base_path

        mock_blob = Mock()
        mock_blob_factory.return_value = mock_blob
        backend = GCSBackend(app=self.app)
        backend.get(b"mykey")

        mock_blob_factory.assert_called_once()
        # mock_blob_factory.assert_called_with(
        #     blob=base_path + "mykey", container="celery"
        # )
        #
        # mock_blob.get_blob_client.return_value.download_blob.return_value.readall.return_value.decode.assert_called_once()

    #
    # @mock_gcs
    # def test_get_a_missing_key(self):
    #     self._mock_gcs_resource()
    #
    #     self.app.conf.gcs_access_key_id = 'somekeyid'
    #     self.app.conf.gcs_secret_access_key = 'somesecret'
    #     self.app.conf.gcs_bucket = 'bucket'
    #
    #     gcs_backend = GCSBackend(app=self.app)
    #     result = gcs_backend.get('uuidddd')
    #
    #     assert result is None
    #
    # @patch('celery.backends.gcs.boto3')
    # def test_with_error_while_getting_key(self, mock_boto3):
    #     error = ClientError(
    #         {'Error': {'Code': '403', 'Message': 'Permission denied'}}, 'error'
    #     )
    #     mock_boto3.Session().resource().Object().load.side_effect = error
    #
    #     self.app.conf.gcs_access_key_id = 'somekeyid'
    #     self.app.conf.gcs_secret_access_key = 'somesecret'
    #     self.app.conf.gcs_bucket = 'bucket'
    #
    #     gcs_backend = GCSBackend(app=self.app)
    #
    #     with pytest.raises(ClientError):
    #         gcs_backend.get('uuidddd')
    #
    # @pytest.mark.parametrize("key", ['uuid', b'uuid'])
    # @mock_gcs
    # def test_delete_a_key(self, key):
    #     self._mock_gcs_resource()
    #
    #     self.app.conf.gcs_access_key_id = 'somekeyid'
    #     self.app.conf.gcs_secret_access_key = 'somesecret'
    #     self.app.conf.gcs_bucket = 'bucket'
    #
    #     gcs_backend = GCSBackend(app=self.app)
    #     gcs_backend._set_with_state(key, 'another_status', states.SUCCESS)
    #     assert gcs_backend.get(key) == 'another_status'
    #
    #     gcs_backend.delete(key)
    #
    #     assert gcs_backend.get(key) is None
    #
    # @mock_gcs
    # def test_with_a_non_existing_bucket(self):
    #     self._mock_gcs_resource()
    #
    #     self.app.conf.gcs_access_key_id = 'somekeyid'
    #     self.app.conf.gcs_secret_access_key = 'somesecret'
    #     self.app.conf.gcs_bucket = 'bucket_not_exists'
    #
    #     gcs_backend = GCSBackend(app=self.app)
    #
    #     with pytest.raises(
    #         ClientError, match=r'.*The specified bucket does not exist'
    #     ):
    #         gcs_backend._set_with_state(
    #             'uuid', 'another_status', states.SUCCESS
    #         )
    #
    # def _mock_gcs_resource(self):
    #     # Create AWS gcs Bucket for moto.
    #     session = boto3.Session(
    #         gcp_access_key_id='moto_key_id',
    #         gcp_secret_access_key='moto_secret_key',
    #         region_name='us-east-1',
    #     )
    #     gcs = session.resource('gcs')
    #     gcs.create_bucket(Bucket='bucket')
