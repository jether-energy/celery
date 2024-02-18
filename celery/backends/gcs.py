"""GCS result store backend."""
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import RLock

import requests

from celery.exceptions import ImproperlyConfigured
from kombu.utils import cached_property
from kombu.utils.encoding import bytes_to_str
from .base import KeyValueStoreBackend

try:
    from google.cloud import storage
    from google.api_core.retry import Retry
    from google.cloud.storage import Client
    from google.cloud.storage.constants import _DEFAULT_TIMEOUT
except ImportError:
    storage = None

__all__ = ('GCSBackend',)


class GCSBackend(KeyValueStoreBackend):
    """Google Cloud Storage task result backend."""

    _lock = RLock()
    _my_pid = os.getpid()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if not storage:
            raise ImproperlyConfigured(
                'You must install google-cloud-storage to use gcs backend'
            )
        conf = self.app.conf

        self.bucket_name = conf.get('gcs_bucket')
        if not self.bucket_name:
            raise ImproperlyConfigured(
                'Missing bucket name: specify gcs_bucket to use gcs backend'
            )
        self.project = conf.get('gcs_project')
        if not self.project:
            raise ImproperlyConfigured(
                'Missing project:specify gcs_project to use gcs backend'
            )
        self.base_path = conf.get('gcs_base_path', '').strip('/')
        self.ttl = float(conf.get('gcs_ttl') or 0)
        if self.ttl < 0:
            raise ImproperlyConfigured(
                'Invalid ttl:gcs_ttl must be greater than or equal to 0'
            )

        self._connect_timeout = conf.get(
            'gcs_connect_timeout', _DEFAULT_TIMEOUT
        )
        self._read_timeout = conf.get('gcs_read_timeout', _DEFAULT_TIMEOUT)
        self._client = None

    def get(self, key):
        key = bytes_to_str(key)
        blob = self._get_blob(key)
        try:
            return blob.download_as_bytes()
        except storage.blob.NotFound:
            return None

    def set(self, key, value):
        key = bytes_to_str(key)
        blob = self._get_blob(key)
        if self.ttl:
            blob.custom_time = datetime.utcnow() + timedelta(seconds=self.ttl)
        Retry()(blob.upload_from_string)(value)

    def delete(self, key):
        key = bytes_to_str(key)
        blob = self._get_blob(key)
        if blob.exists():
            Retry()(blob.delete)()

    def mget(self, keys):
        with ThreadPoolExecutor() as pool:
            return list(pool.map(self.get, keys))

    @property
    def client(self):
        """Returns a storage client."""

        if self._client:
            return self._client

        # make sure it's thread-safe, as creating a new client is expensive
        with self._lock:
            if self._client and self._my_pid == os.getpid():
                return self._client
            # Make sure each process gets its own connection after a fork
            self._client = Client(project=self.project)
            self._my_pid = os.getpid()

            # Increase the number of connections to the server
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=128, pool_maxsize=128, max_retries=3
            )
            self._client._http.mount("https://", adapter)
            self._client._http._auth_request.session.mount("https://", adapter)

            return self._client

    @cached_property
    def bucket(self):
        timeout = (self._connect_timeout, self._read_timeout)
        return self.client.get_bucket(self.bucket_name, timeout=timeout)

    def _get_blob(self, key):
        key_bucket_path = f'{self.base_path}/{key}' if self.base_path else key
        return self.bucket.blob(key_bucket_path)
