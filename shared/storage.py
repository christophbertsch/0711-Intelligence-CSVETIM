"""
Object storage utilities for the CSV Import Guardian Agent System.
"""

import hashlib
import logging
import mimetypes
import os
from datetime import datetime, timedelta
from io import BytesIO
from typing import BinaryIO, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, NoCredentialsError
from minio import Minio
from minio.error import S3Error

from shared.config import get_s3_config, get_settings

logger = logging.getLogger(__name__)


class StorageManager:
    """S3-compatible object storage manager."""
    
    def __init__(self):
        self.settings = get_settings()
        self.s3_config = get_s3_config()
        self.s3_client = None
        self.minio_client = None
        self.bucket_name = self.settings.s3_bucket_name
        
    async def initialize(self):
        """Initialize storage clients."""
        try:
            # Initialize boto3 S3 client
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.s3_config["endpoint_url"],
                aws_access_key_id=self.s3_config["aws_access_key_id"],
                aws_secret_access_key=self.s3_config["aws_secret_access_key"],
                region_name=self.s3_config["region_name"],
                use_ssl=self.s3_config["use_ssl"],
                config=Config(
                    signature_version='s3v4',
                    retries={'max_attempts': 3}
                )
            )
            
            # Initialize MinIO client for advanced operations
            endpoint = urlparse(self.s3_config["endpoint_url"])
            self.minio_client = Minio(
                endpoint.netloc,
                access_key=self.s3_config["aws_access_key_id"],
                secret_key=self.s3_config["aws_secret_access_key"],
                secure=self.s3_config["use_ssl"]
            )
            
            # Ensure bucket exists
            await self.ensure_bucket_exists()
            
            logger.info(f"Storage manager initialized with bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize storage manager: {e}")
            raise
    
    async def ensure_bucket_exists(self):
        """Ensure the default bucket exists."""
        try:
            # Check if bucket exists using MinIO client
            if not self.minio_client.bucket_exists(self.bucket_name):
                logger.info(f"Creating bucket: {self.bucket_name}")
                self.minio_client.make_bucket(self.bucket_name)
                
                # Set bucket policy for public read access to exports
                await self.set_bucket_policy()
                
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise
    
    async def set_bucket_policy(self):
        """Set bucket policy for appropriate access control."""
        # Basic policy allowing read access to exports folder
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/exports/*"
                }
            ]
        }
        
        try:
            import json
            self.minio_client.set_bucket_policy(
                self.bucket_name,
                json.dumps(policy)
            )
        except Exception as e:
            logger.warning(f"Could not set bucket policy: {e}")
    
    def generate_object_key(
        self,
        client_id: str,
        ingest_id: str,
        filename: str,
        prefix: str = "uploads"
    ) -> str:
        """Generate a consistent object key for storage."""
        # Clean filename
        clean_filename = "".join(c for c in filename if c.isalnum() or c in ".-_")
        
        # Generate key with hierarchy
        timestamp = datetime.utcnow().strftime("%Y/%m/%d")
        return f"{prefix}/{client_id}/{timestamp}/{ingest_id}/{clean_filename}"
    
    async def upload_file(
        self,
        file_obj: BinaryIO,
        object_key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload a file to object storage."""
        try:
            # Detect content type if not provided
            if not content_type:
                content_type, _ = mimetypes.guess_type(object_key)
                if not content_type:
                    content_type = "application/octet-stream"
            
            # Prepare metadata
            s3_metadata = metadata or {}
            s3_metadata.update({
                "uploaded_at": datetime.utcnow().isoformat(),
                "content_type": content_type,
            })
            
            # Calculate file size and hash
            file_obj.seek(0)
            content = file_obj.read()
            file_size = len(content)
            file_hash = hashlib.sha256(content).hexdigest()
            
            s3_metadata.update({
                "file_size": str(file_size),
                "sha256": file_hash,
            })
            
            # Upload file
            file_obj.seek(0)
            self.s3_client.upload_fileobj(
                file_obj,
                self.bucket_name,
                object_key,
                ExtraArgs={
                    "ContentType": content_type,
                    "Metadata": s3_metadata,
                }
            )
            
            # Generate URI
            uri = f"s3://{self.bucket_name}/{object_key}"
            logger.info(f"Uploaded file to {uri} (size: {file_size} bytes)")
            
            return uri
            
        except Exception as e:
            logger.error(f"Error uploading file {object_key}: {e}")
            raise
    
    async def download_file(self, object_key: str) -> Tuple[BytesIO, Dict[str, str]]:
        """Download a file from object storage."""
        try:
            # Get object
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            
            # Read content
            content = response['Body'].read()
            file_obj = BytesIO(content)
            
            # Extract metadata
            metadata = response.get('Metadata', {})
            metadata.update({
                'content_type': response.get('ContentType', ''),
                'content_length': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified', ''),
            })
            
            logger.debug(f"Downloaded file {object_key} ({len(content)} bytes)")
            return file_obj, metadata
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                raise FileNotFoundError(f"Object not found: {object_key}")
            logger.error(f"Error downloading file {object_key}: {e}")
            raise
    
    async def delete_file(self, object_key: str):
        """Delete a file from object storage."""
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            logger.info(f"Deleted file: {object_key}")
            
        except Exception as e:
            logger.error(f"Error deleting file {object_key}: {e}")
            raise
    
    async def list_files(
        self,
        prefix: str = "",
        max_keys: int = 1000
    ) -> List[Dict[str, any]]:
        """List files in object storage."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            for obj in response.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj['ETag'].strip('"'),
                })
            
            return files
            
        except Exception as e:
            logger.error(f"Error listing files with prefix {prefix}: {e}")
            raise
    
    async def generate_presigned_url(
        self,
        object_key: str,
        expiration: int = 3600,
        http_method: str = "GET"
    ) -> str:
        """Generate a presigned URL for object access."""
        try:
            url = self.s3_client.generate_presigned_url(
                http_method.lower() + '_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': object_key
                },
                ExpiresIn=expiration
            )
            
            logger.debug(f"Generated presigned URL for {object_key}")
            return url
            
        except Exception as e:
            logger.error(f"Error generating presigned URL for {object_key}: {e}")
            raise
    
    async def generate_upload_url(
        self,
        object_key: str,
        content_type: str = "application/octet-stream",
        expiration: int = 3600
    ) -> str:
        """Generate a presigned URL for file upload."""
        try:
            url = self.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': object_key,
                    'ContentType': content_type,
                },
                ExpiresIn=expiration
            )
            
            logger.debug(f"Generated upload URL for {object_key}")
            return url
            
        except Exception as e:
            logger.error(f"Error generating upload URL for {object_key}: {e}")
            raise
    
    async def copy_file(self, source_key: str, dest_key: str):
        """Copy a file within the storage bucket."""
        try:
            copy_source = {
                'Bucket': self.bucket_name,
                'Key': source_key
            }
            
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            
            logger.info(f"Copied file from {source_key} to {dest_key}")
            
        except Exception as e:
            logger.error(f"Error copying file from {source_key} to {dest_key}: {e}")
            raise
    
    async def get_file_metadata(self, object_key: str) -> Dict[str, any]:
        """Get metadata for a file without downloading it."""
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=object_key
            )
            
            metadata = {
                'content_type': response.get('ContentType', ''),
                'content_length': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified', ''),
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {}),
            }
            
            return metadata
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise FileNotFoundError(f"Object not found: {object_key}")
            logger.error(f"Error getting metadata for {object_key}: {e}")
            raise
    
    async def file_exists(self, object_key: str) -> bool:
        """Check if a file exists in storage."""
        try:
            await self.get_file_metadata(object_key)
            return True
        except FileNotFoundError:
            return False
        except Exception:
            return False


# Global storage manager instance
_storage_manager: Optional[StorageManager] = None


async def get_storage_manager() -> StorageManager:
    """Get the global storage manager instance."""
    global _storage_manager
    
    if _storage_manager is None:
        _storage_manager = StorageManager()
        await _storage_manager.initialize()
    
    return _storage_manager


# Utility functions

def extract_object_key_from_uri(uri: str) -> str:
    """Extract object key from S3 URI."""
    if uri.startswith("s3://"):
        parts = uri[5:].split("/", 1)
        if len(parts) == 2:
            return parts[1]
    raise ValueError(f"Invalid S3 URI: {uri}")


def generate_file_hash(file_obj: BinaryIO) -> str:
    """Generate SHA256 hash of a file."""
    file_obj.seek(0)
    hash_sha256 = hashlib.sha256()
    
    for chunk in iter(lambda: file_obj.read(4096), b""):
        hash_sha256.update(chunk)
    
    file_obj.seek(0)
    return hash_sha256.hexdigest()


async def upload_text_content(
    content: str,
    object_key: str,
    content_type: str = "text/plain",
    encoding: str = "utf-8"
) -> str:
    """Upload text content to storage."""
    storage_manager = await get_storage_manager()
    
    content_bytes = content.encode(encoding)
    file_obj = BytesIO(content_bytes)
    
    return await storage_manager.upload_file(
        file_obj,
        object_key,
        content_type=content_type,
        metadata={"encoding": encoding}
    )


async def download_text_content(object_key: str, encoding: str = "utf-8") -> str:
    """Download text content from storage."""
    storage_manager = await get_storage_manager()
    
    file_obj, metadata = await storage_manager.download_file(object_key)
    content_bytes = file_obj.read()
    
    # Use encoding from metadata if available
    if "encoding" in metadata:
        encoding = metadata["encoding"]
    
    return content_bytes.decode(encoding)


# Health check functions

async def check_storage_health() -> dict:
    """Check storage connectivity and bucket access."""
    try:
        storage_manager = await get_storage_manager()
        
        # Test bucket access
        test_key = "health-check/test.txt"
        test_content = f"Health check at {datetime.utcnow().isoformat()}"
        
        # Upload test file
        await upload_text_content(test_content, test_key)
        
        # Download test file
        downloaded_content = await download_text_content(test_key)
        
        # Clean up test file
        await storage_manager.delete_file(test_key)
        
        # Verify content
        content_match = downloaded_content == test_content
        
        return {
            "status": "healthy",
            "bucket": storage_manager.bucket_name,
            "upload": "ok",
            "download": "ok",
            "content_integrity": "ok" if content_match else "error",
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }


# File type detection and validation

ALLOWED_FILE_TYPES = {
    "text/csv": [".csv"],
    "application/vnd.ms-excel": [".xls"],
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"],
    "text/tab-separated-values": [".tsv"],
    "text/plain": [".txt"],
    "application/zip": [".zip"],
}


def validate_file_type(filename: str, content_type: str) -> bool:
    """Validate if file type is allowed."""
    if content_type in ALLOWED_FILE_TYPES:
        allowed_extensions = ALLOWED_FILE_TYPES[content_type]
        file_extension = os.path.splitext(filename)[1].lower()
        return file_extension in allowed_extensions
    
    return False


def detect_file_encoding(file_obj: BinaryIO) -> str:
    """Detect file encoding."""
    import chardet
    
    file_obj.seek(0)
    raw_data = file_obj.read(10000)  # Read first 10KB
    file_obj.seek(0)
    
    result = chardet.detect(raw_data)
    encoding = result.get('encoding', 'utf-8')
    
    # Fallback to common encodings if detection fails
    if not encoding or result.get('confidence', 0) < 0.7:
        for fallback_encoding in ['utf-8', 'iso-8859-1', 'windows-1252']:
            try:
                raw_data.decode(fallback_encoding)
                encoding = fallback_encoding
                break
            except UnicodeDecodeError:
                continue
    
    return encoding or 'utf-8'


# Cleanup utilities

async def cleanup_old_files(
    prefix: str,
    days_old: int = 30,
    dry_run: bool = True
) -> List[str]:
    """Clean up files older than specified days."""
    storage_manager = await get_storage_manager()
    cutoff_date = datetime.utcnow() - timedelta(days=days_old)
    
    files_to_delete = []
    files = await storage_manager.list_files(prefix=prefix)
    
    for file_info in files:
        if file_info['last_modified'] < cutoff_date:
            files_to_delete.append(file_info['key'])
    
    if not dry_run:
        for object_key in files_to_delete:
            try:
                await storage_manager.delete_file(object_key)
                logger.info(f"Deleted old file: {object_key}")
            except Exception as e:
                logger.error(f"Error deleting {object_key}: {e}")
    
    return files_to_delete


# Batch operations

async def upload_multiple_files(
    files: List[Tuple[BinaryIO, str, Optional[str]]],
    base_prefix: str = "batch"
) -> List[str]:
    """Upload multiple files in batch."""
    storage_manager = await get_storage_manager()
    uploaded_uris = []
    
    for file_obj, filename, content_type in files:
        object_key = f"{base_prefix}/{filename}"
        uri = await storage_manager.upload_file(file_obj, object_key, content_type)
        uploaded_uris.append(uri)
    
    return uploaded_uris


async def download_multiple_files(
    object_keys: List[str]
) -> List[Tuple[str, BytesIO, Dict[str, str]]]:
    """Download multiple files in batch."""
    storage_manager = await get_storage_manager()
    downloaded_files = []
    
    for object_key in object_keys:
        try:
            file_obj, metadata = await storage_manager.download_file(object_key)
            downloaded_files.append((object_key, file_obj, metadata))
        except Exception as e:
            logger.error(f"Error downloading {object_key}: {e}")
            # Continue with other files
    
    return downloaded_files