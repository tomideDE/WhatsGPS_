import os
import asyncio
import aiohttp
import aiomysql
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import jwt
import json
from dataclasses import dataclass
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('whatsgps_updater.log')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    charset: str = 'utf8mb4'

@dataclass
class APIConfig:
    url: str
    username: str
    password: str
    target_user_id: str

@dataclass
class ProcessingConfig:
    batch_size: int = 50
    max_retries: int = 3
    request_timeout: int = 30

@dataclass
class Tracker:
    staff_id: int
    imei: str

@dataclass
class APITrackerData:
    imei: str
    heartTime: int
    online: int
    lon: float
    lat: float
    speed: float
    carId: Optional[str] = None
    
    # Store any extra fields the API returns
    extra_data: Dict[str, Any] = None

    def __init__(self, **kwargs):
        # Extract the fields we care about
        self.imei = kwargs.get('imei')
        self.heartTime = kwargs.get('heartTime')
        self.online = kwargs.get('online')
        self.lon = kwargs.get('lon')
        self.lat = kwargs.get('lat')
        self.speed = kwargs.get('speed')
        self.carId = kwargs.get('carId')
        
        # Store any extra fields
        expected_fields = {'imei', 'heartTime', 'online', 'lon', 'lat', 'speed', 'carId'}
        self.extra_data = {k: v for k, v in kwargs.items() if k not in expected_fields}
        
        if self.extra_data:
            logger.info(f"Extra fields from API: {list(self.extra_data.keys())}")

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]):
        """Create instance from API response"""
        return cls(**data)

@dataclass
class MileageData:
    mileage: float
    day: str

class WhatsGPSTrackerUpdater:
    def __init__(self):
        self.db_config = DatabaseConfig(
            host=os.getenv('DB_HOST', '34.159.134.5'),
            user=os.getenv('DB_USER', 'bi_team'),
            password=os.getenv('DB_PASSWORD', 'G&rojGxQkhg2dUYm'),
            database=os.getenv('DB_NAME', 'bulkbana_apps'),
            port=int(os.getenv('DB_PORT', '3306'))
        )
        
        self.api_config = APIConfig(
            url=os.getenv('WHATSGPS_API_URL', 'https://www.whatsgps.com'),
            username=os.getenv('WHATSGPS_USERNAME', 'BG_Fleet01'),
            password=os.getenv('WHATSGPS_PASSWORD', '123456aa'),
            target_user_id=os.getenv('WHATSGPS_TARGET_USER_ID', '38777')
        )
        
        self.processing_config = ProcessingConfig(
            batch_size=int(os.getenv('BATCH_SIZE', '10')),
            max_retries=int(os.getenv('MAX_RETRIES', '3'))
        )
        
        self.db_pool = None
        self.session = None
        self.token = None
        self.token_expiry = None
        
    async def connect_to_database(self) -> None:
        """Connect to MySQL database"""
        try:
            self.db_pool = await aiomysql.create_pool(
                host=self.db_config.host,
                user=self.db_config.user,
                password=self.db_config.password,
                db=self.db_config.database,
                port=self.db_config.port,
                charset=self.db_config.charset,
                autocommit=True
            )
            logger.info("Connected to MySQL database")
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

    async def close_database(self) -> None:
        """Close database connection"""
        if self.db_pool:
            self.db_pool.close()
            await self.db_pool.wait_closed()
            logger.info("Database connection closed")

    async def create_session(self) -> None:
        """Create aiohttp session"""
        timeout = aiohttp.ClientTimeout(total=self.processing_config.request_timeout)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close_session(self) -> None:
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            logger.info("HTTP session closed")

    async def get_trackers_from_database(self) -> List[Tracker]:
        """Fetch all trackers from database"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(
                        'SELECT staff_id, imei FROM asset_gps_tracker'
                    )
                    rows = await cursor.fetchall()
                    logger.info(f"Found {len(rows)} trackers in database")
                    return [Tracker(staff_id=row['staff_id'], imei=row['imei']) for row in rows]
        except Exception as e:
            logger.error(f"Failed to fetch trackers from database: {str(e)}")
            raise

    async def authenticate_with_api(self) -> str:
        """Authenticate with WhatsGPS API and get token"""
        if not self.api_config.username or not self.api_config.password:
            raise ValueError("WhatsGPS username and password are required")

        url = f"{self.api_config.url}/user/login.do"
        params = {
            'name': self.api_config.username,
            'password': self.api_config.password
        }

        logger.info("Authenticating with WhatsGPS API...")
        
        try:
            async with self.session.get(url, params=params) as response:
                data = await response.json()
                
                if data.get('ret') != 1:
                    error_code = data.get('code', 'Unknown error')
                    raise Exception(f"Authentication failed: {error_code}")

                self.token = data['data']['token']
                
                # Parse JWT token to get expiry
                try:
                    payload = jwt.decode(self.token, options={"verify_signature": False})
                    self.token_expiry = datetime.fromtimestamp(payload['exp'])
                    logger.info(f"Authentication successful. Token expires: {self.token_expiry.isoformat()}")
                except Exception as jwt_error:
                    # If we can't parse JWT, assume 1 hour validity
                    self.token_expiry = datetime.now() + timedelta(hours=1)
                    logger.info("Authentication successful. Token expiry unknown, assuming 1 hour validity.")

                return self.token
                
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise

    def is_token_valid(self) -> bool:
        """Check if token is still valid"""
        if not self.token or not self.token_expiry:
            return False
        
        # Check if token expires within the next 5 minutes
        five_minutes_from_now = datetime.now() + timedelta(minutes=5)
        return self.token_expiry > five_minutes_from_now

    async def ensure_valid_token(self) -> None:
        """Ensure we have a valid token, re-authenticate if needed"""
        if not self.is_token_valid():
            logger.info("Token expired or invalid, re-authenticating...")
            await self.authenticate_with_api()

    async def fetch_tracker_data_from_api(self) -> List[APITrackerData]:
        """Fetch tracker data from WhatsGPS API"""
        await self.ensure_valid_token()

        if not self.api_config.target_user_id:
            raise ValueError("WhatsGPS target_user_id is required")

        url = f"{self.api_config.url}/carStatus/getByUserId.do"
        params = {
            'token': self.token,
            'targetUserId': self.api_config.target_user_id
        }

        logger.info("Fetching tracker data from WhatsGPS API...")
        
        try:
            async with self.session.get(url, params=params) as response:
                data = await response.json()
                
                if data.get('ret') != 1:
                    # Handle authentication errors
                    if data.get('code') in ['401', '403'] or any(auth_term in data.get('message', '').lower() 
                            for auth_term in ['token', 'auth', 'unauthorized']):
                        logger.info("Token appears to be invalid, re-authenticating...")
                        await self.authenticate_with_api()
                        
                        # Retry with new token
                        retry_params = {
                            'token': self.token,
                            'targetUserId': self.api_config.target_user_id
                        }
                        async with self.session.get(url, params=retry_params) as retry_response:
                            retry_data = await retry_response.json()
                            
                            if retry_data.get('ret') != 1:
                                raise Exception(f"API returned error after re-authentication: {retry_data.get('code', 'Unknown error')}")
                            
                            logger.info(f"Retrieved {len(retry_data['data'])} trackers from API (after re-auth)")
                            return [APITrackerData.from_api_response(tracker) for tracker in data['data']]
                    
                    raise Exception(f"API returned error: {data.get('code', 'Unknown error')}")

                logger.info(f"Retrieved {len(data['data'])} trackers from API")
                return [APITrackerData.from_api_response(tracker) for tracker in data['data']]
                
        except Exception as e:
            logger.error(f"Failed to fetch data from WhatsGPS API: {str(e)}")
            raise

    def get_current_day_time_range(self) -> Tuple[str, str]:
        """Get start and end time for current day"""
        now = datetime.now()
        date_str = now.strftime('%Y-%m-%d')
        
        start_time = f"{date_str} 00:00:00"
        end_time = now.strftime('%Y-%m-%d %H:%M:%S')
        
        return start_time, end_time

    async def fetch_mileage_data_from_api(self, car_id: str) -> Optional[MileageData]:
        """Fetch mileage data for a specific car"""
        await self.ensure_valid_token()

        start_time, end_time = self.get_current_day_time_range()
        url = f"{self.api_config.url}/position/mileageStaByDay.do"
        params = {
            'token': self.token,
            'carId': car_id,
            'startTime': start_time,
            'endTime': end_time
        }

        logger.info(f"Fetching mileage data for carId {car_id} ({start_time} to {end_time})...")
        
        try:
            async with self.session.get(url, params=params) as response:
                data = await response.json()
                
                if data.get('ret') != 1:
                    # Handle authentication errors
                    if data.get('code') in ['401', '403'] or any(auth_term in data.get('message', '').lower() 
                            for auth_term in ['token', 'auth', 'unauthorized']):
                        logger.info("Token appears to be invalid for mileage request, re-authenticating...")
                        await self.authenticate_with_api()
                        
                        # Retry with new token
                        retry_params = {
                            'token': self.token,
                            'carId': car_id,
                            'startTime': start_time,
                            'endTime': end_time
                        }
                        async with self.session.get(url, params=retry_params) as retry_response:
                            retry_data = await retry_response.json()
                            
                            if retry_data.get('ret') != 1:
                                raise Exception(f"Mileage API returned error after re-authentication: {retry_data.get('code', 'Unknown error')}")
                            
                            return self.process_mileage_response(retry_data.get('data', []), car_id)
                    
                    raise Exception(f"Mileage API returned error: {data.get('code', 'Unknown error')}")

                return self.process_mileage_response(data.get('data', []), car_id)
                
        except Exception as e:
            logger.error(f"Failed to fetch mileage data for carId {car_id}: {str(e)}")
            return None

    def process_mileage_response(self, mileage_data_array: List[Dict], car_id: str) -> Optional[MileageData]:
        """Process mileage API response and calculate total for today"""
        if not mileage_data_array or not isinstance(mileage_data_array, list):
            logger.info(f"No mileage data found for carId {car_id}")
            return None

        # Get today's date
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Find all mileage data entries for today
        today_mileage_entries = [item for item in mileage_data_array if item.get('day') == today]
        
        if not today_mileage_entries:
            logger.info(f"No mileage data found for today ({today}) for carId {car_id}")
            return None
        
        # Sum up all mileage entries for today
        total_mileage = sum(entry.get('mileage', 0) for entry in today_mileage_entries)
        
        logger.info(f"Found {len(today_mileage_entries)} mileage entries for today ({today}) for carId {car_id}: Total = {total_mileage} miles")
        
        # Return the first entry but with summed mileage
        return MileageData(
            mileage=total_mileage,
            day=today
        )

    def calculate_offline_duration(self, heart_time: int, online: int) -> str:
        """Calculate human-readable offline duration"""
        if online == 1:
            return "0"  # Online

        current_time = int(datetime.now().timestamp() * 1000)
        offline_duration_ms = current_time - heart_time
        
        # Convert to human readable format
        seconds = offline_duration_ms // 1000
        minutes = seconds // 60
        hours = minutes // 60
        days = hours // 24

        if days > 0:
            return f"{days}d {hours % 24}h {minutes % 60}m"
        elif hours > 0:
            return f"{hours}h {minutes % 60}m"
        elif minutes > 0:
            return f"{minutes}m {seconds % 60}s"
        else:
            return f"{seconds}s"

    async def update_tracker_in_database(self, tracker_data: APITrackerData, mileage_data: Optional[MileageData] = None) -> bool:
        """Update tracker information in database"""
        try:
            last_online = datetime.fromtimestamp(tracker_data.heartTime / 1000)
            offline_duration = self.calculate_offline_duration(tracker_data.heartTime, tracker_data.online)
            online_status = 'Online' if tracker_data.online == 1 else 'Offline'
            
            # Extract mileage value
            mileage = mileage_data.mileage if mileage_data else None

            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        """UPDATE asset_gps_tracker 
                         SET online = %s, last_online = %s, offline_duration = %s, 
                             longitude = %s, latitude = %s, speed = %s, carId = %s, 
                             mileage = %s, date_updated = NOW() 
                         WHERE imei = %s""",
                        [online_status, last_online, offline_duration, tracker_data.lon, 
                         tracker_data.lat, tracker_data.speed, tracker_data.carId, 
                         mileage, tracker_data.imei]
                    )

            mileage_info = f", Mileage: {mileage}" if mileage is not None else ''
            logger.info(f"Updated tracker {tracker_data.imei}: {online_status} (Last online: {last_online.isoformat()}{mileage_info})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update tracker {tracker_data.imei}: {str(e)}")
            return False

    async def process_trackers(self) -> Dict[str, int]:
        """Main processing function"""
        try:
            # Get trackers from database
            db_trackers = await self.get_trackers_from_database()
            db_imei_set = {tracker.imei for tracker in db_trackers}

            # Get tracker data from API
            api_trackers = await self.fetch_tracker_data_from_api()
            
            # Filter API trackers to only include those in our database
            relevant_trackers = [tracker for tracker in api_trackers if tracker.imei in db_imei_set]

            logger.info(f"Processing {len(relevant_trackers)} relevant trackers...")

            success_count = 0
            error_count = 0
            mileage_fetch_count = 0
            mileage_error_count = 0

            # Process trackers in batches
            for i in range(0, len(relevant_trackers), self.processing_config.batch_size):
                batch = relevant_trackers[i:i + self.processing_config.batch_size]
                
                logger.info(f"Processing batch {i // self.processing_config.batch_size + 1}/{(len(relevant_trackers) - 1) // self.processing_config.batch_size + 1}")
                
                batch_tasks = []
                for tracker in batch:
                    task = self.process_single_tracker(
                        tracker
                    )
                    batch_tasks.append(task)
                
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        error_count += 1
                        logger.error(f" Batch item failed: {str(result)}")
                    elif result:
                        success_count += 1
                    else:
                        error_count += 1

                # Update counts from tasks
                for result in results:
                    if not isinstance(result, Exception):
                        try:
                            success, mfc, mec = result
                            mileage_fetch_count += mfc
                            mileage_error_count += mec
                        except:
                            pass

                # Small delay between batches
                if i + self.processing_config.batch_size < len(relevant_trackers):
                    await asyncio.sleep(0.2)

            logger.info(f"\n Update Summary:")
            logger.info(f" Successfully updated: {success_count}")
            logger.info(f" Failed updates: {error_count}")
            logger.info(f" Total processed: {len(relevant_trackers)}")
            logger.info(f" Mileage data fetched: {mileage_fetch_count}")
            logger.info(f" Mileage fetch errors: {mileage_error_count}")

            return {
                'success_count': success_count,
                'error_count': error_count,
                'total_processed': len(relevant_trackers),
                'mileage_fetch_count': mileage_fetch_count,
                'mileage_error_count': mileage_error_count
            }
            
        except Exception as e:
            logger.error(f"Error processing trackers: {str(e)}")
            raise

    async def process_single_tracker(self, tracker: APITrackerData) -> Tuple[bool, int, int]:
        """Process a single tracker with mileage data"""
        mileage_data = None
        mileage_fetch_count = 0
        mileage_error_count = 0
        
        # Fetch mileage data if carId is available
        if tracker.carId:
            try:
                mileage_data = await self.fetch_mileage_data_from_api(tracker.carId)
                if mileage_data:
                    mileage_fetch_count = 1
                else:
                    mileage_error_count = 1
            except Exception as e:
                logger.error(f" Mileage fetch failed for carId {tracker.carId}, continuing with tracker update...")
                mileage_error_count = 1
        
        # Update tracker with mileage data
        success = await self.update_tracker_in_database(tracker, mileage_data)
        return success, mileage_fetch_count, mileage_error_count

    async def run(self) -> Dict[str, int]:
        """Main execution method"""
        try:
            logger.info("Starting WhatsGPS Tracker Updater...")
            logger.info(f"Started at: {datetime.now().isoformat()}")
            
            await self.create_session()
            await self.connect_to_database()
            await self.authenticate_with_api()
            
            results = await self.process_trackers()
            
            logger.info(f"\n Update completed successfully!")
            logger.info(f"Finished at: {datetime.now().isoformat()}")
            
            return results
            
        except Exception as e:
            logger.error(f"Fatal error: {str(e)}")
            raise
        finally:
            await self.close_session()
            await self.close_database()


async def main():
    """Main execution function"""
    updater = WhatsGPSTrackerUpdater()
    
    try:
        await updater.run()
        return 0
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        return 1


def signal_handler(signum, frame):
    """Handle graceful shutdown"""
    logger.info(f'\n Received signal {signum}, shutting down gracefully...')
    sys.exit(0)


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the async main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)