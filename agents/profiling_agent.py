"""
Profiling Agent - Analyzes file structure and data quality.
"""

import csv
import hashlib
import logging
import re
from collections import Counter, defaultdict
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from chardet import detect

from shared.base_agent import BaseAgent, register_agent
from shared.events import BaseEvent, FileReceivedEvent, FileProfiledEvent
from shared.storage import detect_file_encoding

logger = logging.getLogger(__name__)


@register_agent("profiling")
class ProfilingAgent(BaseAgent):
    """
    ProfilingAgent analyzes uploaded files to detect structure and quality.
    
    Responsibilities:
    - Detect file encoding, delimiter, and structure
    - Analyze column types and statistics
    - Generate data quality metrics
    - Store profiling results
    - Emit file.profiled events
    """
    
    def __init__(self, agent_name: str):
        super().__init__(agent_name)
        self.sample_size = self.settings.sample_size
        self.common_delimiters = [',', ';', '\t', '|', ':']
        self.date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}\.\d{2}\.\d{4}',  # DD.MM.YYYY
        ]
    
    async def handle_event(self, event: BaseEvent):
        """Handle incoming events."""
        if isinstance(event, FileReceivedEvent):
            await self._profile_file(event)
        else:
            logger.warning(f"Unhandled event type: {event.event_type}")
    
    async def _profile_file(self, event: FileReceivedEvent):
        """Profile a received file."""
        try:
            logger.info(f"Profiling file for client {event.client_id}, ingest {event.ingest_id}")
            
            # Download file from storage
            object_key = self._extract_object_key(event.uri)
            file_obj, metadata = await self.storage_manager.download_file(object_key)
            
            # Detect file characteristics
            file_characteristics = await self._detect_file_characteristics(file_obj, event.filename)
            
            # Profile the data
            profile_results = await self._profile_data(file_obj, file_characteristics)
            
            # Store profiling results
            await self._store_profile_results(event, file_characteristics, profile_results)
            
            # Emit file.profiled event
            await self._emit_file_profiled_event(event, file_characteristics, profile_results)
            
            logger.info(f"Successfully profiled file: {event.ingest_id}")
            
        except Exception as e:
            logger.error(f"Error profiling file: {e}")
            await self._handle_profiling_error(event, str(e))
    
    def _extract_object_key(self, uri: str) -> str:
        """Extract object key from S3 URI."""
        if uri.startswith("s3://"):
            parts = uri[5:].split("/", 1)
            if len(parts) == 2:
                return parts[1]
        raise ValueError(f"Invalid S3 URI: {uri}")
    
    async def _detect_file_characteristics(self, file_obj, filename: str) -> Dict[str, Any]:
        """Detect file encoding, delimiter, and basic structure."""
        file_obj.seek(0)
        
        # Detect encoding
        raw_sample = file_obj.read(10000)
        file_obj.seek(0)
        
        encoding_result = detect(raw_sample)
        encoding = encoding_result.get('encoding', 'utf-8')
        encoding_confidence = encoding_result.get('confidence', 0.0)
        
        # Fallback encoding detection
        if encoding_confidence < 0.7:
            encoding = detect_file_encoding(file_obj)
        
        # Read file content
        file_obj.seek(0)
        try:
            content = file_obj.read().decode(encoding)
        except UnicodeDecodeError:
            # Fallback to utf-8 with error handling
            file_obj.seek(0)
            content = file_obj.read().decode('utf-8', errors='replace')
            encoding = 'utf-8'
        
        # Detect delimiter and structure
        delimiter, has_header = self._detect_csv_structure(content)
        
        # Count rows and estimate structure
        lines = content.split('\n')
        non_empty_lines = [line.strip() for line in lines if line.strip()]
        
        return {
            'encoding': encoding,
            'encoding_confidence': encoding_confidence,
            'delimiter': delimiter,
            'has_header': has_header,
            'total_lines': len(lines),
            'non_empty_lines': len(non_empty_lines),
            'estimated_rows': len(non_empty_lines) - (1 if has_header else 0),
            'content_sample': content[:1000],  # First 1KB for analysis
        }
    
    def _detect_csv_structure(self, content: str) -> Tuple[str, bool]:
        """Detect CSV delimiter and whether file has header."""
        # Get first few lines for analysis
        lines = content.split('\n')[:10]
        non_empty_lines = [line for line in lines if line.strip()]
        
        if len(non_empty_lines) < 2:
            return ',', True  # Default assumptions
        
        # Try to detect delimiter using csv.Sniffer
        try:
            sniffer = csv.Sniffer()
            sample = '\n'.join(non_empty_lines[:5])
            dialect = sniffer.sniff(sample, delimiters=',;\t|:')
            delimiter = dialect.delimiter
            
            # Check if first line looks like a header
            has_header = sniffer.has_header(sample)
            
        except Exception:
            # Fallback: count delimiter occurrences
            delimiter_counts = {}
            for delim in self.common_delimiters:
                counts = [line.count(delim) for line in non_empty_lines[:5]]
                if counts and all(c > 0 for c in counts):
                    # Check consistency
                    if len(set(counts)) == 1:  # All lines have same count
                        delimiter_counts[delim] = counts[0]
            
            if delimiter_counts:
                delimiter = max(delimiter_counts.items(), key=lambda x: x[1])[0]
            else:
                delimiter = ','  # Default
            
            # Simple header detection: check if first row has different characteristics
            has_header = self._detect_header_heuristic(non_empty_lines, delimiter)
        
        return delimiter, has_header
    
    def _detect_header_heuristic(self, lines: List[str], delimiter: str) -> bool:
        """Heuristic to detect if first line is a header."""
        if len(lines) < 2:
            return True
        
        first_row = lines[0].split(delimiter)
        second_row = lines[1].split(delimiter)
        
        if len(first_row) != len(second_row):
            return True  # Different column counts suggest header
        
        # Check if first row contains mostly text while second contains numbers
        first_numeric = sum(1 for cell in first_row if self._is_numeric(cell.strip()))
        second_numeric = sum(1 for cell in second_row if self._is_numeric(cell.strip()))
        
        # If first row has significantly fewer numbers, it's likely a header
        if len(first_row) > 0 and first_numeric / len(first_row) < 0.3 and second_numeric / len(second_row) > 0.5:
            return True
        
        return False  # Default to no header
    
    def _is_numeric(self, value: str) -> bool:
        """Check if a string represents a numeric value."""
        try:
            float(value.replace(',', ''))  # Handle comma thousands separator
            return True
        except ValueError:
            return False
    
    async def _profile_data(self, file_obj, characteristics: Dict[str, Any]) -> Dict[str, Any]:
        """Profile the data content."""
        file_obj.seek(0)
        content = file_obj.read().decode(characteristics['encoding'])
        
        # Parse CSV data
        try:
            df = pd.read_csv(
                StringIO(content),
                delimiter=characteristics['delimiter'],
                header=0 if characteristics['has_header'] else None,
                nrows=self.sample_size,
                low_memory=False,
                dtype=str  # Read everything as string initially
            )
        except Exception as e:
            logger.warning(f"Error parsing CSV with pandas: {e}")
            # Fallback to basic parsing
            return await self._basic_data_profile(content, characteristics)
        
        # Generate column statistics
        column_stats = {}
        sample_rows = []
        
        for col in df.columns:
            stats = self._analyze_column(df[col])
            column_stats[str(col)] = stats
        
        # Generate sample rows
        sample_rows = df.head(min(10, len(df))).to_dict('records')
        
        # Calculate overall quality score
        quality_score = self._calculate_quality_score(df, column_stats)
        
        # Detect potential issues
        issues = self._detect_data_issues(df, column_stats)
        
        return {
            'columns': list(df.columns),
            'column_count': len(df.columns),
            'row_count': len(df),
            'column_stats': column_stats,
            'sample_rows': sample_rows,
            'quality_score': quality_score,
            'issues': issues,
            'data_types': self._suggest_data_types(column_stats),
        }
    
    async def _basic_data_profile(self, content: str, characteristics: Dict[str, Any]) -> Dict[str, Any]:
        """Basic data profiling when pandas fails."""
        lines = content.split('\n')
        non_empty_lines = [line.strip() for line in lines if line.strip()]
        
        if not non_empty_lines:
            return {
                'columns': [],
                'column_count': 0,
                'row_count': 0,
                'column_stats': {},
                'sample_rows': [],
                'quality_score': 0.0,
                'issues': ['File appears to be empty'],
                'data_types': {},
            }
        
        delimiter = characteristics['delimiter']
        has_header = characteristics['has_header']
        
        # Parse first row to get columns
        first_row = non_empty_lines[0].split(delimiter)
        columns = [f"col_{i}" for i in range(len(first_row))]
        
        if has_header:
            columns = [cell.strip() for cell in first_row]
            data_rows = non_empty_lines[1:]
        else:
            data_rows = non_empty_lines
        
        # Basic statistics
        sample_rows = []
        for i, line in enumerate(data_rows[:10]):
            cells = line.split(delimiter)
            row_dict = {}
            for j, cell in enumerate(cells):
                col_name = columns[j] if j < len(columns) else f"col_{j}"
                row_dict[col_name] = cell.strip()
            sample_rows.append(row_dict)
        
        return {
            'columns': columns,
            'column_count': len(columns),
            'row_count': len(data_rows),
            'column_stats': {},  # Would need more complex parsing
            'sample_rows': sample_rows,
            'quality_score': 0.5,  # Default score
            'issues': ['Basic profiling only - detailed analysis failed'],
            'data_types': {},
        }
    
    def _analyze_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze a single column."""
        stats = {
            'name': series.name,
            'total_count': len(series),
            'null_count': series.isnull().sum(),
            'empty_count': (series == '').sum(),
            'unique_count': series.nunique(),
            'most_common': None,
            'data_type_guess': 'string',
            'patterns': [],
        }
        
        # Calculate completeness
        stats['completeness'] = 1.0 - (stats['null_count'] + stats['empty_count']) / stats['total_count']
        
        # Get most common values
        if stats['unique_count'] > 0:
            value_counts = series.value_counts().head(5)
            stats['most_common'] = value_counts.to_dict()
        
        # Analyze non-null, non-empty values
        clean_values = series.dropna()
        clean_values = clean_values[clean_values != '']
        
        if len(clean_values) > 0:
            # Guess data type
            stats['data_type_guess'] = self._guess_data_type(clean_values)
            
            # Detect patterns
            stats['patterns'] = self._detect_patterns(clean_values)
            
            # Type-specific statistics
            if stats['data_type_guess'] == 'numeric':
                try:
                    numeric_values = pd.to_numeric(clean_values, errors='coerce')
                    numeric_values = numeric_values.dropna()
                    if len(numeric_values) > 0:
                        stats.update({
                            'min_value': float(numeric_values.min()),
                            'max_value': float(numeric_values.max()),
                            'mean_value': float(numeric_values.mean()),
                            'std_value': float(numeric_values.std()),
                        })
                except Exception:
                    pass
            
            elif stats['data_type_guess'] == 'string':
                lengths = clean_values.str.len()
                stats.update({
                    'min_length': int(lengths.min()),
                    'max_length': int(lengths.max()),
                    'avg_length': float(lengths.mean()),
                })
        
        return stats
    
    def _guess_data_type(self, values: pd.Series) -> str:
        """Guess the data type of a column."""
        sample = values.head(min(100, len(values)))
        
        # Check for numeric
        numeric_count = 0
        for val in sample:
            if self._is_numeric(str(val)):
                numeric_count += 1
        
        if numeric_count / len(sample) > 0.8:
            return 'numeric'
        
        # Check for dates
        date_count = 0
        for val in sample:
            if self._is_date_like(str(val)):
                date_count += 1
        
        if date_count / len(sample) > 0.8:
            return 'date'
        
        # Check for boolean
        boolean_values = {'true', 'false', 'yes', 'no', '1', '0', 'y', 'n'}
        boolean_count = sum(1 for val in sample if str(val).lower().strip() in boolean_values)
        
        if boolean_count / len(sample) > 0.8:
            return 'boolean'
        
        return 'string'
    
    def _is_date_like(self, value: str) -> bool:
        """Check if a string looks like a date."""
        for pattern in self.date_patterns:
            if re.match(pattern, value.strip()):
                return True
        return False
    
    def _detect_patterns(self, values: pd.Series) -> List[Dict[str, Any]]:
        """Detect common patterns in string values."""
        patterns = []
        sample = values.head(min(100, len(values)))
        
        # Common patterns to check
        pattern_checks = [
            (r'^[A-Z]{2,3}\d{3,}$', 'product_code'),
            (r'^\d{8,14}$', 'barcode_ean'),
            (r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', 'email'),
            (r'^\+?[\d\s\-\(\)]{7,}$', 'phone'),
            (r'^\d{4}-\d{2}-\d{2}$', 'date_iso'),
            (r'^\d+\.\d{2}$', 'currency'),
        ]
        
        for pattern, pattern_name in pattern_checks:
            matches = sum(1 for val in sample if re.match(pattern, str(val).strip()))
            if matches > 0:
                confidence = matches / len(sample)
                patterns.append({
                    'pattern': pattern_name,
                    'regex': pattern,
                    'matches': matches,
                    'confidence': confidence,
                })
        
        return sorted(patterns, key=lambda x: x['confidence'], reverse=True)
    
    def _calculate_quality_score(self, df: pd.DataFrame, column_stats: Dict[str, Any]) -> float:
        """Calculate overall data quality score."""
        if df.empty:
            return 0.0
        
        scores = []
        
        # Completeness score
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isnull().sum().sum()
        empty_cells = (df == '').sum().sum()
        completeness = 1.0 - (null_cells + empty_cells) / total_cells
        scores.append(completeness)
        
        # Consistency score (based on data type consistency)
        consistency_scores = []
        for col_name, stats in column_stats.items():
            if stats['total_count'] > 0:
                # Score based on how well values match the guessed type
                type_consistency = 1.0 - (stats['null_count'] + stats['empty_count']) / stats['total_count']
                consistency_scores.append(type_consistency)
        
        if consistency_scores:
            scores.append(sum(consistency_scores) / len(consistency_scores))
        
        # Uniqueness score (penalize too many duplicates)
        uniqueness_scores = []
        for col_name, stats in column_stats.items():
            if stats['total_count'] > 0:
                uniqueness = min(1.0, stats['unique_count'] / stats['total_count'])
                uniqueness_scores.append(uniqueness)
        
        if uniqueness_scores:
            scores.append(sum(uniqueness_scores) / len(uniqueness_scores))
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def _detect_data_issues(self, df: pd.DataFrame, column_stats: Dict[str, Any]) -> List[str]:
        """Detect potential data quality issues."""
        issues = []
        
        # Check for completely empty columns
        for col_name, stats in column_stats.items():
            if stats['completeness'] == 0:
                issues.append(f"Column '{col_name}' is completely empty")
            elif stats['completeness'] < 0.5:
                issues.append(f"Column '{col_name}' has low completeness ({stats['completeness']:.1%})")
        
        # Check for duplicate headers
        columns = list(df.columns)
        if len(columns) != len(set(columns)):
            duplicates = [col for col in set(columns) if columns.count(col) > 1]
            issues.append(f"Duplicate column names found: {duplicates}")
        
        # Check for suspicious column names
        suspicious_patterns = [r'^Unnamed:', r'^Column\d+$', r'^\d+$']
        for col in columns:
            for pattern in suspicious_patterns:
                if re.match(pattern, str(col)):
                    issues.append(f"Suspicious column name: '{col}'")
                    break
        
        # Check for very wide tables
        if len(columns) > 100:
            issues.append(f"Very wide table with {len(columns)} columns")
        
        # Check for very sparse data
        if df.shape[0] > 0:
            sparsity = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
            if sparsity > 0.7:
                issues.append(f"Very sparse data ({sparsity:.1%} missing values)")
        
        return issues
    
    def _suggest_data_types(self, column_stats: Dict[str, Any]) -> Dict[str, str]:
        """Suggest appropriate data types for columns."""
        suggestions = {}
        
        for col_name, stats in column_stats.items():
            data_type = stats.get('data_type_guess', 'string')
            
            # Refine suggestions based on patterns
            patterns = stats.get('patterns', [])
            if patterns:
                top_pattern = patterns[0]
                if top_pattern['confidence'] > 0.8:
                    if top_pattern['pattern'] == 'barcode_ean':
                        data_type = 'string'  # Keep as string to preserve leading zeros
                    elif top_pattern['pattern'] == 'date_iso':
                        data_type = 'date'
                    elif top_pattern['pattern'] == 'currency':
                        data_type = 'decimal'
            
            suggestions[col_name] = data_type
        
        return suggestions
    
    async def _store_profile_results(self, event: FileReceivedEvent, characteristics: Dict[str, Any], profile: Dict[str, Any]):
        """Store profiling results in the database."""
        async with self.database_session() as session:
            # Create profile JSON
            profile_data = {
                **characteristics,
                **profile,
                'profiled_at': datetime.utcnow().isoformat(),
            }
            
            # Update stg_file with profile information
            query = """
                UPDATE stg_file 
                SET 
                    encoding = :encoding,
                    delimiter = :delimiter,
                    has_header = :has_header,
                    row_count = :row_count,
                    column_count = :column_count,
                    profile = :profile,
                    status = 'profiled',
                    updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
                "encoding": characteristics['encoding'],
                "delimiter": characteristics['delimiter'],
                "has_header": characteristics['has_header'],
                "row_count": profile['row_count'],
                "column_count": profile['column_count'],
                "profile": profile_data,
            })
            
            await session.commit()
    
    async def _emit_file_profiled_event(self, event: FileReceivedEvent, characteristics: Dict[str, Any], profile: Dict[str, Any]):
        """Emit file.profiled event."""
        file_profiled_event = FileProfiledEvent(
            event_id=f"profiled-{event.ingest_id}",
            trace_id=event.trace_id,
            client_id=event.client_id,
            ingest_id=event.ingest_id,
            source_agent=self.agent_name,
            idempotency_key=f"profiled-{event.client_id}-{event.ingest_id}",
            columns=profile['columns'],
            encoding=characteristics['encoding'],
            delimiter=characteristics['delimiter'],
            has_header=characteristics['has_header'],
            row_count=profile['row_count'],
            column_count=profile['column_count'],
            sample_rows=profile['sample_rows'],
            column_stats=profile['column_stats'],
            quality_score=profile['quality_score'],
            issues=profile['issues'],
        )
        
        await self.publish_event(file_profiled_event)
    
    async def _handle_profiling_error(self, event: FileReceivedEvent, error_message: str):
        """Handle profiling errors."""
        logger.error(f"Profiling error for {event.ingest_id}: {error_message}")
        
        # Update database status
        async with self.database_session() as session:
            query = """
                UPDATE stg_file 
                SET status = 'profiling_failed', 
                    updated_at = NOW()
                WHERE client_id = :client_id AND ingest_id = :ingest_id
            """
            
            await session.execute(query, {
                "client_id": event.client_id,
                "ingest_id": event.ingest_id,
            })
            
            await session.commit()
    
    async def on_initialize(self):
        """Initialize agent-specific components."""
        logger.info("ProfilingAgent initialized")
        
        # Could initialize ML models for better type detection
        # Could set up caching for common patterns