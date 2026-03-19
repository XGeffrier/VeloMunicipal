import os.path

from google.cloud import storage


class StorageClient:
    PROJECT_NAME = "fausse-commune"
    DEFAULT_BUCKET_NAME = "velo_municipal_bucket"

    _client = None
    _default_bucket = None

    @classmethod
    def upload_file(cls, gs_path: str, local_path: str,
                    content_type: str = None, make_public: bool = False) -> None:
        """
        Upload a file to Google Cloud Storage.
        If content_type is None, it will be guessed from the file extension.
        Raise exception if the urser does not have permission to write to the bucket.
        """
        bucket = cls._get_default_bucket()
        blob = bucket.blob(cls._clean_path(gs_path))
        blob.upload_from_filename(local_path, content_type=content_type, timeout=300)
        if make_public:
            blob.make_public()

    @classmethod
    def download_file(cls, gs_path: str, local_path: str):
        """
        Downloads a blob into memory.
        Raise exception if the blob does not exist or if the user does not have permission to read it.
        """
        bucket = cls._get_default_bucket()
        blob = bucket.blob(cls._clean_path(gs_path))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        blob.download_to_filename(local_path)

    @classmethod
    def empty_files(cls):
        bucket = cls._get_default_bucket()
        blobs = bucket.list_blobs()
        for blob in blobs:
            blob.delete()

    @classmethod
    def _get_client(cls) -> storage.Client:
        if cls._client is None:
            cls._client = storage.Client(project=cls.PROJECT_NAME)
        return cls._client

    @classmethod
    def _get_default_bucket(cls) -> storage.Bucket:
        client = cls._get_client()
        return client.bucket(cls.DEFAULT_BUCKET_NAME)

    @classmethod
    def _clean_path(cls, path: str):
        """        Return any gs path cleaned from prefixes."""
        if path.startswith('/'):
            path = path[1:]
        elif path.startswith('gs://'):
            path = path[5:]
        if path.startswith(f"{cls.DEFAULT_BUCKET_NAME}/"):
            path = path[len(f"{cls.DEFAULT_BUCKET_NAME}/"):]
        return path
