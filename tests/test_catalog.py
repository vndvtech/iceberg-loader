"""Tests for iceberg_loader.catalog."""

from unittest.mock import patch

import pytest

from iceberg_loader.catalog import get_rest_catalog


class TestGetRestCatalog:
    def test_raises_without_uri(self) -> None:
        """get_rest_catalog raises ValueError when no URI is provided."""
        with patch.dict('os.environ', {}, clear=True), pytest.raises(ValueError, match='REST Catalog URI is required'):
            get_rest_catalog()

    def test_uri_from_env(self) -> None:
        """URI is read from ICEBERG_REST_URI env var."""
        with patch(
            'iceberg_loader.catalog.RestCatalog',
        ) as mock_rest:
            with patch.dict(
                'os.environ',
                {
                    'ICEBERG_REST_URI': 'http://localhost:8181/api',
                    'ICEBERG_WAREHOUSE': 's3://bucket/warehouse/',
                    'ICEBERG_CREDENTIAL': 'user:pass',
                },
                clear=True,
            ):
                get_rest_catalog()

            mock_rest.assert_called_once()
            _, kwargs = mock_rest.call_args
            assert kwargs['uri'] == 'http://localhost:8181/api'
            assert kwargs['warehouse'] == 's3://bucket/warehouse/'
            assert kwargs['credential'] == 'user:pass'

    def test_uri_from_arg_overrides_env(self) -> None:
        """Explicit uri arg takes precedence over env var."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict('os.environ', {'ICEBERG_REST_URI': 'http://env-uri/api'}, clear=True):
                get_rest_catalog(uri='http://arg-uri/api')

            _, kwargs = mock_rest.call_args
            assert kwargs['uri'] == 'http://arg-uri/api'

    def test_s3_properties_from_env(self) -> None:
        """S3 properties are read from env vars."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {
                    'ICEBERG_REST_URI': 'http://localhost:8181/api',
                    'S3_ENDPOINT': 'http://minio:9000',
                    'S3_ACCESS_KEY': 'minio',
                    'S3_SECRET_KEY': 'minio123',
                    'S3_REGION': 'us-west-2',
                },
                clear=True,
            ):
                get_rest_catalog()

            _, kwargs = mock_rest.call_args
            assert kwargs['s3.endpoint'] == 'http://minio:9000'
            assert kwargs['s3.access-key-id'] == 'minio'
            assert kwargs['s3.secret-access-key'] == 'minio123'
            assert kwargs['s3.region'] == 'us-west-2'
            assert kwargs['s3.path-style-access'] == 'true'

    def test_s3_properties_from_args(self) -> None:
        """S3 properties from explicit args take precedence."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {
                    'ICEBERG_REST_URI': 'http://localhost:8181/api',
                    'S3_ENDPOINT': 'http://env-endpoint:9000',
                },
                clear=True,
            ):
                get_rest_catalog(s3_endpoint='http://arg-endpoint:9001')

            _, kwargs = mock_rest.call_args
            assert kwargs['s3.endpoint'] == 'http://arg-endpoint:9001'

    def test_missing_optional_env_vars_are_skipped(self) -> None:
        """Missing optional env vars are not included in properties."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog()

            _, kwargs = mock_rest.call_args
            assert 'warehouse' not in kwargs
            assert 'credential' not in kwargs
            assert 's3.endpoint' not in kwargs

    def test_extra_properties_merged(self) -> None:
        """extra_properties are passed through."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog(extra_properties={'custom.prop': 'value'})

            _, kwargs = mock_rest.call_args
            assert kwargs['custom.prop'] == 'value'

    def test_custom_name(self) -> None:
        """Custom catalog name is used."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog(name='my-catalog')

            _, kwargs = mock_rest.call_args
            assert kwargs['name'] == 'my-catalog'

    def test_default_name(self) -> None:
        """Default catalog name is 'rest-catalog'."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog()

            _, kwargs = mock_rest.call_args
            assert kwargs['name'] == 'rest-catalog'

    def test_extra_properties_override_explicit_args(self) -> None:
        """extra_properties take precedence over explicit s3_* args."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog(
                    s3_endpoint='http://arg-endpoint:9001',
                    s3_region='us-west-2',
                    extra_properties={
                        's3.endpoint': 'http://override-endpoint:9002',
                        's3.region': 'eu-central-1',
                    },
                )

            _, kwargs = mock_rest.call_args
            assert kwargs['s3.endpoint'] == 'http://override-endpoint:9002'
            assert kwargs['s3.region'] == 'eu-central-1'

    def test_raises_with_empty_uri_and_empty_env(self) -> None:
        """get_rest_catalog raises ValueError for an empty uri with no env fallback."""
        with patch.dict('os.environ', {}, clear=True), pytest.raises(ValueError, match='REST Catalog URI is required'):
            get_rest_catalog(uri='')

    def test_s3_path_style_enabled_explicitly(self) -> None:
        """s3_path_style=True sets s3.path-style-access even without an endpoint."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog(s3_path_style=True)

            _, kwargs = mock_rest.call_args
            assert kwargs['s3.path-style-access'] == 'true'

    def test_s3_path_style_enabled_by_endpoint(self) -> None:
        """An s3_endpoint implies path-style access (MinIO-compatible)."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog(s3_endpoint='http://minio:9000')

            _, kwargs = mock_rest.call_args
            assert kwargs['s3.path-style-access'] == 'true'

    def test_s3_path_style_absent_without_endpoint(self) -> None:
        """Without an endpoint or explicit flag, path-style access is not set."""
        with patch('iceberg_loader.catalog.RestCatalog') as mock_rest:
            with patch.dict(
                'os.environ',
                {'ICEBERG_REST_URI': 'http://localhost:8181/api'},
                clear=True,
            ):
                get_rest_catalog()

            _, kwargs = mock_rest.call_args
            assert 's3.path-style-access' not in kwargs
