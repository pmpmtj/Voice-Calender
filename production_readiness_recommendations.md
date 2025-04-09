# Production Readiness Recommendations

## 1. Schema Improvements
```sql
-- Improved table schema
CREATE TABLE calendar_events (
    id SERIAL PRIMARY KEY,
    summary TEXT NOT NULL,
    location TEXT,
    description TEXT,
    start_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    end_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    attendees JSONB,  -- Using JSONB instead of TEXT
    recurrence JSONB,
    reminders JSONB,
    visibility VARCHAR(50) CHECK (visibility IN ('default', 'public', 'private')),
    color_id VARCHAR(50),
    transparency VARCHAR(50) CHECK (transparency IN ('opaque', 'transparent')),
    status VARCHAR(50) CHECK (status IN ('confirmed', 'tentative', 'cancelled')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_datetime_range CHECK (end_datetime > start_datetime)
);

-- Additional indexes
CREATE INDEX idx_calendar_events_end_datetime ON calendar_events(end_datetime);
CREATE INDEX idx_calendar_events_status ON calendar_events(status);
CREATE INDEX idx_calendar_events_date_range ON calendar_events(start_datetime, end_datetime);
```

## 2. Database Migrations
```python
# Using Alembic for migrations
from alembic import op
import sqlalchemy as sa

def upgrade():
    # Example migration
    op.create_table(
        'calendar_events_audit',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('event_id', sa.Integer, sa.ForeignKey('calendar_events.id')),
        sa.Column('action', sa.String(50)),
        sa.Column('changed_at', sa.DateTime(timezone=True))
    )
```

## 3. Connection Management
```python
# Enhanced connection pool configuration
connection_pool = pool.SimpleConnectionPool(
    minconn=2,          # Minimum connections
    maxconn=20,         # Maximum connections
    connection_factory=None,
    cursor_factory=None,
    async_=False,
    maxusage=None,      # Maximum number of reuses
    setsession=[],      # SQL commands executed when connection is created
    reset=True,         # Reset connection on return to pool
    host='localhost',
    port=5432,
    **connection_parameters
)

# Add connection retry mechanism
def get_connection_with_retry(max_retries=3, retry_delay=1):
    for attempt in range(max_retries):
        try:
            return get_connection()
        except psycopg2.Error as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay * (attempt + 1))
```

## 4. Monitoring and Metrics
```python
# Add monitoring
from prometheus_client import Counter, Histogram
import time

DB_OPERATION_DURATION = Histogram(
    'db_operation_duration_seconds',
    'Time spent in database operations',
    ['operation_type']
)

DB_ERRORS = Counter(
    'db_operation_errors_total',
    'Total number of database errors',
    ['error_type']
)

def monitor_db_operation(operation_type):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                DB_OPERATION_DURATION.labels(
                    operation_type=operation_type
                ).observe(time.time() - start_time)
                return result
            except Exception as e:
                DB_ERRORS.labels(
                    error_type=type(e).__name__
                ).inc()
                raise
        return wrapper
    return decorator
```

## 5. Data Validation
```python
from pydantic import BaseModel, validator
from datetime import datetime
from typing import Optional, List, Dict

class CalendarEvent(BaseModel):
    summary: str
    start_datetime: datetime
    end_datetime: datetime
    location: Optional[str] = None
    description: Optional[str] = None
    attendees: Optional[List[Dict[str, str]]] = None
    
    @validator('end_datetime')
    def end_after_start(cls, v, values):
        if 'start_datetime' in values and v <= values['start_datetime']:
            raise ValueError('end_datetime must be after start_datetime')
        return v
```

## 6. Testing Setup
```python
# Integration tests
import pytest
from datetime import datetime, timedelta

@pytest.fixture
def test_db():
    """Provide test database connection"""
    # Setup test database
    test_url = os.getenv('TEST_DATABASE_URL')
    initialize_test_db(test_url)
    yield
    # Cleanup after tests
    cleanup_test_db()

def test_save_calendar_event(test_db):
    """Test saving calendar event"""
    event_data = {
        'summary': 'Test Event',
        'start_datetime': datetime.now(),
        'end_datetime': datetime.now() + timedelta(hours=1)
    }
    event_id = save_calendar_event(**event_data)
    assert event_id is not None
    
    # Verify saved event
    saved_event = get_calendar_event(event_id)
    assert saved_event['summary'] == event_data['summary']
```

## 7. High Availability Configuration
```python
# Example configuration for high availability
DB_CONFIG = {
    'primary': {
        'host': 'primary.db.example.com',
        'port': 5432,
        'database': 'calendar_db'
    },
    'replica': {
        'host': 'replica.db.example.com',
        'port': 5432,
        'database': 'calendar_db'
    },
    'failover': {
        'max_retries': 3,
        'retry_delay': 1,
        'timeout': 5
    }
}
```

## Implementation Priority

1. **High Priority (Week 1-2)**
   - Schema improvements
   - Data validation
   - Basic testing setup

2. **Medium Priority (Week 3-4)**
   - Database migrations
   - Connection management improvements
   - Monitoring setup

3. **Lower Priority (Week 5-6)**
   - High availability configuration
   - Advanced testing
   - Performance optimization

## Additional Recommendations

1. **Documentation**
   - Add API documentation
   - Document database schema
   - Create maintenance procedures
   - Add deployment guides

2. **Backup Strategy**
   - Implement automated backups
   - Define backup retention policy
   - Create restore procedures
   - Test recovery process

3. **Security Enhancements**
   - Implement row-level security
   - Add audit logging
   - Encrypt sensitive data
   - Regular security reviews 