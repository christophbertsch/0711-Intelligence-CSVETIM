#!/usr/bin/env python3
"""
Comprehensive test script for the CSV Import Guardian Agent System.
"""

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_BASE_URL = "http://localhost:8000"


async def test_api_health():
    """Test API health endpoints."""
    logger.info("Testing API health...")
    
    async with httpx.AsyncClient() as client:
        # Test basic health
        response = await client.get(f"{API_BASE_URL}/health")
        if response.status_code == 200:
            logger.info("✅ Health check passed")
            health_data = response.json()
            logger.info(f"   Status: {health_data.get('status')}")
            
            # Check components
            components = health_data.get('components', {})
            for component, status in components.items():
                component_status = status.get('status', 'unknown')
                if component_status == 'healthy':
                    logger.info(f"   ✅ {component}: {component_status}")
                else:
                    logger.warning(f"   ⚠️  {component}: {component_status}")
        else:
            logger.error(f"❌ Health check failed: {response.status_code}")
            return False
        
        # Test liveness
        response = await client.get(f"{API_BASE_URL}/health/live")
        if response.status_code == 200:
            logger.info("✅ Liveness check passed")
        else:
            logger.error(f"❌ Liveness check failed: {response.status_code}")
        
        # Test readiness
        response = await client.get(f"{API_BASE_URL}/health/ready")
        if response.status_code == 200:
            logger.info("✅ Readiness check passed")
        else:
            logger.warning(f"⚠️  Readiness check failed: {response.status_code}")
    
    return True


async def test_file_upload():
    """Test file upload functionality."""
    logger.info("Testing file upload...")
    
    # Create a simple test CSV
    test_csv_content = """SKU,Name,Brand,Price
TEST-001,Test Product 1,TestBrand,10.50
TEST-002,Test Product 2,TestBrand,15.75
TEST-003,Test Product 3,TestBrand,8.25"""
    
    test_file_path = Path("test_upload.csv")
    test_file_path.write_text(test_csv_content)
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Upload file
            with open(test_file_path, 'rb') as f:
                files = {'file': ('test_upload.csv', f, 'text/csv')}
                data = {'client_id': 'test-client'}
                
                response = await client.post(
                    f"{API_BASE_URL}/v1/csv-import/upload",
                    files=files,
                    data=data
                )
            
            if response.status_code == 200:
                upload_data = response.json()
                job_id = upload_data.get('job_id')
                ingest_id = upload_data.get('ingest_id')
                
                logger.info(f"✅ File upload successful")
                logger.info(f"   Job ID: {job_id}")
                logger.info(f"   Ingest ID: {ingest_id}")
                
                return job_id, ingest_id
            else:
                logger.error(f"❌ File upload failed: {response.status_code}")
                logger.error(f"   Response: {response.text}")
                return None, None
                
    finally:
        # Clean up test file
        if test_file_path.exists():
            test_file_path.unlink()


async def test_job_progress(job_id):
    """Test job progress tracking."""
    logger.info(f"Testing job progress for {job_id}...")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{API_BASE_URL}/v1/csv-import/jobs/{job_id}/progress")
        
        if response.status_code == 200:
            progress_data = response.json()
            status = progress_data.get('status')
            current_stage = progress_data.get('current_stage')
            stages_completed = progress_data.get('stages_completed', [])
            
            logger.info(f"✅ Job progress retrieved")
            logger.info(f"   Status: {status}")
            logger.info(f"   Current stage: {current_stage}")
            logger.info(f"   Completed stages: {', '.join(stages_completed)}")
            
            return progress_data
        else:
            logger.error(f"❌ Job progress failed: {response.status_code}")
            return None


async def test_file_preview(job_id):
    """Test file preview functionality."""
    logger.info(f"Testing file preview for {job_id}...")
    
    async with httpx.AsyncClient() as client:
        preview_request = {
            "sample_size": 10,
            "detect_encoding": True,
            "detect_delimiter": True
        }
        
        response = await client.post(
            f"{API_BASE_URL}/v1/csv-import/jobs/{job_id}/preview",
            json=preview_request
        )
        
        if response.status_code == 200:
            preview_data = response.json()
            profile = preview_data.get('profile', {})
            
            logger.info(f"✅ File preview successful")
            logger.info(f"   Encoding: {profile.get('encoding')}")
            logger.info(f"   Delimiter: {profile.get('delimiter')}")
            logger.info(f"   Columns: {len(profile.get('columns', []))}")
            logger.info(f"   Rows: {profile.get('row_count')}")
            logger.info(f"   Quality score: {profile.get('quality_score')}")
            
            return preview_data
        else:
            logger.error(f"❌ File preview failed: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None


async def test_template_suggestions():
    """Test template suggestion functionality."""
    logger.info("Testing template suggestions...")
    
    async with httpx.AsyncClient() as client:
        suggest_request = {
            "columns": ["SKU", "Name", "Brand", "Price", "Length", "Material"],
            "sample_data": [
                {"SKU": "TEST-001", "Name": "Test Product", "Brand": "TestBrand", "Price": "10.50"},
                {"SKU": "TEST-002", "Name": "Another Product", "Brand": "TestBrand", "Price": "15.75"}
            ],
            "etim_class": "EC000123"
        }
        
        response = await client.post(
            f"{API_BASE_URL}/v1/csv-import/templates/suggest",
            json=suggest_request
        )
        
        if response.status_code == 200:
            suggestion_data = response.json()
            suggested_templates = suggestion_data.get('suggested_templates', [])
            field_mappings = suggestion_data.get('field_mappings', [])
            
            logger.info(f"✅ Template suggestions successful")
            logger.info(f"   Suggested templates: {len(suggested_templates)}")
            logger.info(f"   Field mappings: {len(field_mappings)}")
            
            return suggestion_data
        else:
            logger.error(f"❌ Template suggestions failed: {response.status_code}")
            return None


async def test_validation_rules():
    """Test validation rule suggestions."""
    logger.info("Testing validation rule suggestions...")
    
    async with httpx.AsyncClient() as client:
        rule_request = {
            "target_type": "field",
            "target_name": "sku",
            "sample_data": ["TEST-001", "TEST-002", "TEST-003", "PROD-004"],
            "etim_context": {}
        }
        
        response = await client.post(
            f"{API_BASE_URL}/v1/csv-import/suggest/rules",
            json=rule_request
        )
        
        if response.status_code == 200:
            rules_data = response.json()
            suggested_rules = rules_data.get('suggested_rules', [])
            
            logger.info(f"✅ Rule suggestions successful")
            logger.info(f"   Suggested rules: {len(suggested_rules)}")
            
            for rule in suggested_rules:
                logger.info(f"   - {rule.get('rule_type')}: {rule.get('description')}")
            
            return rules_data
        else:
            logger.error(f"❌ Rule suggestions failed: {response.status_code}")
            return None


async def test_export_formats():
    """Test export format information."""
    logger.info("Testing export formats...")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{API_BASE_URL}/v1/exports/formats")
        
        if response.status_code == 200:
            formats_data = response.json()
            formats = formats_data.get('formats', [])
            
            logger.info(f"✅ Export formats retrieved")
            logger.info(f"   Available formats: {len(formats)}")
            
            for fmt in formats:
                logger.info(f"   - {fmt.get('format')}: {fmt.get('name')}")
            
            return formats_data
        else:
            logger.error(f"❌ Export formats failed: {response.status_code}")
            return None


async def test_system_info():
    """Test system information endpoint."""
    logger.info("Testing system info...")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{API_BASE_URL}/v1/info")
        
        if response.status_code == 200:
            info_data = response.json()
            
            logger.info(f"✅ System info retrieved")
            logger.info(f"   Service: {info_data.get('service')}")
            logger.info(f"   Version: {info_data.get('version')}")
            logger.info(f"   Environment: {info_data.get('environment')}")
            logger.info(f"   Features: {len(info_data.get('features', []))}")
            
            return info_data
        else:
            logger.error(f"❌ System info failed: {response.status_code}")
            return None


async def test_job_listing():
    """Test job listing functionality."""
    logger.info("Testing job listing...")
    
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{API_BASE_URL}/v1/csv-import/jobs?limit=10")
        
        if response.status_code == 200:
            jobs_data = response.json()
            jobs = jobs_data.get('jobs', [])
            total_count = jobs_data.get('total_count', 0)
            
            logger.info(f"✅ Job listing successful")
            logger.info(f"   Total jobs: {total_count}")
            logger.info(f"   Jobs returned: {len(jobs)}")
            
            return jobs_data
        else:
            logger.error(f"❌ Job listing failed: {response.status_code}")
            return None


async def run_comprehensive_test():
    """Run comprehensive system test."""
    logger.info("🧪 Starting comprehensive system test...")
    
    test_results = {}
    
    # Test 1: API Health
    test_results['health'] = await test_api_health()
    
    # Test 2: System Info
    test_results['system_info'] = await test_system_info()
    
    # Test 3: Export Formats
    test_results['export_formats'] = await test_export_formats()
    
    # Test 4: File Upload
    job_id, ingest_id = await test_file_upload()
    test_results['upload'] = job_id is not None
    
    if job_id:
        # Test 5: Job Progress
        test_results['progress'] = await test_job_progress(job_id)
        
        # Test 6: File Preview
        test_results['preview'] = await test_file_preview(job_id)
        
        # Wait a bit for processing
        logger.info("⏳ Waiting for processing...")
        await asyncio.sleep(5)
        
        # Test 7: Job Progress Again
        await test_job_progress(job_id)
    
    # Test 8: Template Suggestions
    test_results['template_suggestions'] = await test_template_suggestions()
    
    # Test 9: Validation Rules
    test_results['validation_rules'] = await test_validation_rules()
    
    # Test 10: Job Listing
    test_results['job_listing'] = await test_job_listing()
    
    # Summary
    logger.info("\n📊 Test Summary:")
    passed_tests = sum(1 for result in test_results.values() if result)
    total_tests = len(test_results)
    
    logger.info(f"   Passed: {passed_tests}/{total_tests}")
    
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"   {test_name}: {status}")
    
    if passed_tests == total_tests:
        logger.info("\n🎉 All tests passed! System is working correctly.")
        return True
    else:
        logger.warning(f"\n⚠️  {total_tests - passed_tests} tests failed. Check the logs above.")
        return False


async def wait_for_api():
    """Wait for API to be ready."""
    logger.info("⏳ Waiting for API to be ready...")
    
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{API_BASE_URL}/health/live")
                if response.status_code == 200:
                    logger.info("✅ API is ready!")
                    return True
        except Exception as e:
            logger.debug(f"API not ready yet: {e}")
        
        attempt += 1
        await asyncio.sleep(2)
    
    logger.error("❌ API failed to become ready within timeout")
    return False


async def main():
    """Main test function."""
    logger.info("🚀 CSV Import Guardian Agent System Test Suite")
    
    # Wait for API to be ready
    if not await wait_for_api():
        sys.exit(1)
    
    # Run comprehensive tests
    success = await run_comprehensive_test()
    
    if success:
        logger.info("\n✅ All tests completed successfully!")
        sys.exit(0)
    else:
        logger.error("\n❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())