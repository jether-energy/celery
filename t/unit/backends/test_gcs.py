from unittest.mock import patch, Mock

import pytest
from google.cloud.exceptions import NotFound

from celery.backends.gcs import GCSBackend
from celery.exceptions import ImproperlyConfigured


class test_GCSBackend:
    def setup_method(self):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'

    @pytest.fixture(params=['', 'test_folder/'])
    def base_path(self, request):
        return request.param

    @pytest.fixture(params=[86400, None])
    def ttl(self, request):
        return request.param

    def test_missing_bucket(self):
        self.app.conf.gcs_bucket = None

        with pytest.raises(ImproperlyConfigured, match='Missing bucket name'):
            GCSBackend(app=self.app)

    def test_missing_project(self):
        self.app.conf.gcs_project = None

        with pytest.raises(ImproperlyConfigured, match='Missing project'):
            GCSBackend(app=self.app)

    def test_invalid_ttl(self):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'
        self.app.conf.gcs_ttl = -1

        with pytest.raises(ImproperlyConfigured, match='Invalid ttl'):
            GCSBackend(app=self.app)

    @patch.object(GCSBackend, '_get_blob')
    def test_get_a_key(self, mock_get_blob, base_path):
        self.app.conf.gcs_base_path = base_path

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        backend = GCSBackend(app=self.app)
        backend.get(b"testkey1")

        mock_get_blob.assert_called_once_with('testkey1')
        mock_blob.download_as_bytes.assert_called_once()

    @patch.object(GCSBackend, '_get_blob')
    def test_set_a_key(self, mock_get_blob, base_path, ttl):
        self.app.conf.gcs_base_path = base_path
        self.app.conf.gcs_ttl = ttl

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        backend = GCSBackend(app=self.app)
        backend.set('testkey', 'test-value')
        mock_get_blob.assert_called_once_with('testkey')
        mock_blob.upload_from_string.assert_called_once_with('test-value')
        if ttl:
            assert mock_blob.custom_time is not None

    @patch.object(GCSBackend, '_get_blob')
    def test_get_a_missing_key(self, mock_get_blob):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob

        mock_blob.download_as_bytes.side_effect = NotFound('not found')
        gcs_backend = GCSBackend(app=self.app)
        result = gcs_backend.get('some-key')

        assert result is None

    @patch.object(GCSBackend, '_get_blob')
    def test_delete_an_existing_key(self, mock_get_blob, base_path):
        self.app.conf.gcs_base_path = base_path

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        mock_blob.exists.return_value = True
        backend = GCSBackend(app=self.app)
        backend.delete(b"testkey2")

        mock_get_blob.assert_called_once_with('testkey2')
        mock_blob.exists.assert_called_once()
        mock_blob.delete.assert_called_once()

    def test_mget(self):
        ...

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
