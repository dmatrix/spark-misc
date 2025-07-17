# PyPI Upload Instructions for pyspark-rest-datasource

This document provides step-by-step instructions for uploading the `pyspark-rest-datasource` package to PyPI.

## Prerequisites

1. **PyPI Account**: Create accounts on both Test PyPI and PyPI
   - Test PyPI: https://test.pypi.org/account/register/
   - PyPI: https://pypi.org/account/register/

2. **Install Required Tools**:
   ```bash
   uv add --dev twine
   # or
   pip install twine
   ```

3. **API Tokens**: Create API tokens for secure uploads
   - Test PyPI: https://test.pypi.org/manage/account/token/
   - PyPI: https://pypi.org/manage/account/token/

## 1. Build the Package

```bash
# Clean any previous builds
rm -rf dist/ build/ *.egg-info/

# Build the package
python -m build
```

This creates:
- `dist/pyspark_rest_datasource-0.1.0-py3-none-any.whl` (wheel)
- `dist/pyspark_rest_datasource-0.1.0.tar.gz` (source distribution)

## 2. Test Upload to Test PyPI (Recommended)

First, upload to Test PyPI to verify everything works:

```bash
# Upload to Test PyPI
python -m twine upload --repository testpypi dist/*

# You'll be prompted for:
# Username: __token__
# Password: [your-test-pypi-api-token]
```

### Test Installation from Test PyPI

```bash
# Install from Test PyPI to test
pip install --index-url https://test.pypi.org/simple/ pyspark-rest-datasource

# Test the package
python -c "from pyspark_rest_datasource import RestApiDataSource; print('✅ Package works!')"
```

## 3. Upload to Production PyPI

Once testing is successful:

```bash
# Upload to production PyPI
python -m twine upload dist/*

# You'll be prompted for:
# Username: __token__
# Password: [your-pypi-api-token]
```

## 4. Configure ~/.pypirc (Optional)

Create `~/.pypirc` for easier uploads:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = [your-pypi-api-token]

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = [your-test-pypi-api-token]
```

Then you can upload with:
```bash
twine upload --repository testpypi dist/*  # Test PyPI
twine upload dist/*                        # Production PyPI
```

## 5. Verify Upload

After uploading to PyPI:

1. **Check the package page**: https://pypi.org/project/pyspark-rest-datasource/
2. **Test installation**:
   ```bash
   pip install pyspark-rest-datasource
   ```

## 6. Post-Upload Usage

Users can now install your package with:

```bash
# Using pip
pip install pyspark-rest-datasource

# Using uv
uv add pyspark-rest-datasource
```

## Usage Example

```python
from pyspark.sql import SparkSession
from pyspark_rest_datasource import RestApiDataSource

# Initialize Spark
spark = SparkSession.builder.appName("REST API Example").getOrCreate()

# Register the data source
spark.dataSource.register(RestApiDataSource)

# Read from REST API
df = spark.read \
    .format("restapi") \
    .option("url", "https://jsonplaceholder.typicode.com/users") \
    .option("method", "GET") \
    .load()

df.show()
```

## Package Information

- **Package Name**: `pyspark-rest-datasource`
- **Version**: `0.1.0`
- **Python Requirement**: `>= 3.9`
- **Main Dependencies**: 
  - `pyspark >= 4.0.0`
  - `pyarrow >= 10.0.0`
  - `requests >= 2.25.0`

## Troubleshooting

### Common Issues

1. **403 Forbidden**: Check your API token and permissions
2. **Package name conflict**: The name `pyspark-rest-datasource` should be available
3. **Version conflict**: Increment version in `pyproject.toml` for updates

### Release New Versions

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Rebuild: `python -m build`
4. Upload: `twine upload dist/*`

## Security Notes

- Store API tokens securely
- Use `__token__` as username when prompted
- Consider using environment variables for tokens
- Never commit API tokens to version control

## Resources

- [PyPI Documentation](https://packaging.python.org/tutorials/packaging-projects/)
- [Twine Documentation](https://twine.readthedocs.io/)
- [Python Packaging Guide](https://packaging.python.org/) 