#!/usr/bin/env python3
"""
Aircraft Depot Scheduling System - Refined Version
==================================================

This script processes historical aircraft maintenance data and generates future maintenance
projections for depot-level scheduling. It handles PMI (Periodic Maintenance Inspection)
cycles, 546-day events, rebase operations, and integrates BumbleBee ML predictions.

Key Features:
- Loads historical maintenance data from Excel files
- Processes MAF data for 546-day event detection and cross-referencing
- Generates future PMI events based on 42-month cycles
- Integrates BumbleBee ML flight hour predictions for SLEP calculations
- Generates 546-day projections from MAF data
- Calculates fiscal years/quarters (FY starts Oct 1)
- Processes special 546-day maintenance events from both sources
- Tracks aircraft rebase operations
- Exports clean combined historical and projected data
- Exports separate MAF analysis

REFINEMENTS APPLIED (Version 7.1):
1. Set SLEP scheduling start date to July 2020 as required
2. Updated SLEP durations: SLEP 1-3 = 90 days, MAX_AIRCRAFT_LIFE = 180 days
3. Keep MAF dates as clean strings throughout the process (no pandas Timestamp conversion)
4. Remove problematic date preservation logic that was adding timestamps
5. Clean output file names ending with version only (no UPDATED/FIXED suffixes)
6. Maintain all V7 enhancements (BumbleBee, MAF projections, etc.)

Author: Tim Look
ISO Date: 20250620
Version: 7.1 - Refined SLEP durations and scheduling from July 2020
"""

# Import required libraries
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import logging
from typing import Optional, List, Dict, Tuple

# Configure logging system for debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

###---------------------------------CONFIGURATION SECTION---------------------------------###
# Version and directory management
SCRIPT_VERSION = "7.1"
VERSION_FOLDER_PREFIX = "depot_schedule_v"

# Input file paths (remain in root directory)
INPUT_FILE_DEPOT = 'H60 IMP REPORT 250327.xlsx'
SHEET_NAME_DEPOT = 'Depot Schedule '
INPUT_FILE_MAF = 'Deckplate DEC_2024.csv'
# BumbleBee ML integration
BB_INPUT_FILE = 'ROMEO_predicted_flight_hours_final_v2_dev_local_V2_01.xlsx'

# Output file names (will be placed in version folder) - CLEAN NAMES
OUTPUT_FILE = 'combined_aircraft_data_v7_1.csv'
FUTURE_FILE = 'future_events_v7_1.csv'
REBASE_FILE = 'rebase_events_v7_1.csv'
MAF_ANALYSIS_FILE = 'maf_546_analysis_v7_1.csv'
MAF_PROJECTION_FILE = 'maf_546_projections_v7_1.csv'
SLEP_ANALYSIS_FILE = 'slep_events_v7_1.csv'
LOG_FILE = 'processing_log_v7_1.txt'

# Time-based constants for maintenance scheduling
PROJECTION_END_YEAR = 2052
PMI_INTERVAL_MONTHS = 42
PMI_1_2_INTERVAL = 42
PMI_2_1_INTERVAL = 42
DAY_546_OFFSET = 21  # Updated to 21 days (target completion time from Trevor Choat)
DAY_546_INTERVAL = 546  # 546-day interval for projections

# SLEP flight hour thresholds
SLEP_THRESHOLDS = {
    'SLEP_1': 10000,
    'SLEP_2': 12000,
    'SLEP_3': 14000,
    'MAX_LIFE': 16000
}

###---------------------------------MAIN CLASS DEFINITION---------------------------------###

class AircraftScheduler:
    """
    Refined Aircraft Scheduling System with proper date format preservation and updated SLEP durations.
    """
    
    def __init__(self):
        """
        Initialize the AircraftScheduler object with enhanced data containers.
        """
        # Initialize data storage containers
        self.df = None                      # Historical depot data from Excel (main source)
        self.df_maf = None                  # MAF data loaded separately for analysis
        self.df_bumblebee = None           # BumbleBee ML flight hour predictions
        self.maf_546_events = None         # Extracted 546-day events from MAF data
        self.maf_546_projections = None    # Future 546-day projections from MAF
        self.slep_events = None            # SLEP events from BumbleBee analysis
        self.future_df = None              # Generated future maintenance projections
        self.combined_df = None            # Final combined dataset for export
        
        # Version and directory management
        self.version_folder = None         # Path to version-specific output folder
        self.output_paths = {}             # Dictionary to store full file paths

        # Set up working environment and logging
        self.setup_environment()
    
    def setup_environment(self):
        """
        Set up the working environment for processing including version folder management.
        """
        # Change to script directory for consistent file path handling
        script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        os.chdir(script_dir)
        logger.info(f"Changed working directory to: {script_dir}")
        
        # Create version-specific folder for outputs
        self.setup_version_folder()
        
        # Setup file logging in version folder
        log_path = os.path.join(self.version_folder, LOG_FILE)
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Logging to: {log_path}")
    
    def setup_version_folder(self):
        """
        Create version-specific folder for output files.
        """
        # Generate version folder name
        version_folder_name = f"{VERSION_FOLDER_PREFIX}{SCRIPT_VERSION}"
        self.version_folder = os.path.join(os.getcwd(), version_folder_name)
        
        # Check if version folder already exists
        if os.path.exists(self.version_folder):
            logger.info(f"Version folder already exists: {self.version_folder}")
        else:
            # Create new version folder
            try:
                os.makedirs(self.version_folder)
                logger.info(f"Created new version folder: {self.version_folder}")
            except Exception as e:
                logger.error(f"Error creating version folder: {e}")
                # Fallback to current directory
                self.version_folder = os.getcwd()
        
        # Setup output file paths in version folder
        self.setup_output_paths()
    
    def setup_output_paths(self):
        """
        Setup full paths for all output files in the version folder.
        """
        output_files = {
            'combined': OUTPUT_FILE,
            'future': FUTURE_FILE,
            'rebase': REBASE_FILE,
            'maf_analysis': MAF_ANALYSIS_FILE,
            'maf_projection': MAF_PROJECTION_FILE,
            'slep_analysis': SLEP_ANALYSIS_FILE,
            'log': LOG_FILE
        }
        
        # Create full paths for each output file
        for key, filename in output_files.items():
            self.output_paths[key] = os.path.join(self.version_folder, filename)
        
        logger.info(f"Output files will be saved to: {self.version_folder}")
    
    def load_depot_data(self) -> pd.DataFrame:
        """
        Load and validate depot data from Excel file.
        """
        try:
            logger.info(f"Loading depot data from {INPUT_FILE_DEPOT}...")
            
            if not os.path.exists(INPUT_FILE_DEPOT):
                raise FileNotFoundError(f"Input file '{INPUT_FILE_DEPOT}' not found")
            
            df = pd.read_excel(INPUT_FILE_DEPOT, sheet_name=SHEET_NAME_DEPOT)
            logger.info(f"Successfully loaded {len(df)} depot records")
            
            # Clean column names - Excel often has formatting issues
            df.columns = [col.replace("\n", " ").replace("\r", " ").strip() for col in df.columns]
            
            # Validate the loaded data structure
            self.validate_depot_data(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading depot data: {str(e)}")
            sys.exit(1)
    
    def load_maf_data(self) -> pd.DataFrame:
        """
        Load MAF data for 546-day analysis and AMARG detection.
        """
        try:
            logger.info(f"Loading MAF data from {INPUT_FILE_MAF} for 546-day analysis...")
            
            if not os.path.exists(INPUT_FILE_MAF):
                raise FileNotFoundError(f"MAF file '{INPUT_FILE_MAF}' not found")
            
            df_maf = pd.read_csv(INPUT_FILE_MAF)
            logger.info(f"Successfully loaded {len(df_maf)} MAF records for analysis")
            
            # Clean column names
            df_maf.columns = [col.strip() for col in df_maf.columns]
            
            # Convert Work Center to string to handle both '020' and '20' formats
            if 'Work Center' in df_maf.columns:
                df_maf['Work Center'] = df_maf['Work Center'].astype(str)
            
            self.validate_maf_data(df_maf)
            
            return df_maf
            
        except Exception as e:
            logger.error(f"Error loading MAF data: {str(e)}")
            sys.exit(1)
    
    def load_bumblebee_data(self) -> pd.DataFrame:
        """
        Load BumbleBee ML flight hour predictions for SLEP analysis.
        """
        try:
            logger.info(f"Loading BumbleBee ML data from {BB_INPUT_FILE}...")
            
            if not os.path.exists(BB_INPUT_FILE):
                logger.warning(f"BumbleBee file '{BB_INPUT_FILE}' not found - SLEP analysis will be skipped")
                return pd.DataFrame()
            
            df_bb = pd.read_excel(BB_INPUT_FILE)
            logger.info(f"Successfully loaded {len(df_bb)} BumbleBee ML records")
            
            # Validate required columns
            required_columns = ['AI_Running_Total_Flight_Hours']
            missing_cols = [col for col in required_columns if col not in df_bb.columns]
            
            if missing_cols:
                logger.error(f"Missing required BumbleBee columns: {missing_cols}")
                return pd.DataFrame()
            
            # Look for BUNO column
            buno_columns = [col for col in df_bb.columns if 'buno' in col.lower()]
            if not buno_columns:
                logger.error("No BUNO column found in BumbleBee data")
                return pd.DataFrame()
            
            buno_col = buno_columns[0]
            df_bb['BUNO'] = df_bb[buno_col]
            
            logger.info(f"BumbleBee data validation complete. Unique aircraft: {df_bb['BUNO'].nunique()}")
            
            return df_bb
            
        except Exception as e:
            logger.error(f"Error loading BumbleBee data: {str(e)}")
            return pd.DataFrame()
    
    def validate_depot_data(self, df: pd.DataFrame):
        """
        Validate depot data quality and structure.
        """
        required_columns = ['BUNO', 'START DATE', 'TASK']
        missing_cols = [col for col in required_columns if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required depot columns: {missing_cols}")
        
        for col in required_columns:
            if df[col].isna().all():
                logger.warning(f"Depot column '{col}' is completely empty")
        
        logger.info(f"Depot data validation complete. Unique aircraft: {df['BUNO'].nunique()}")
    
    def validate_maf_data(self, df_maf: pd.DataFrame):
        """
        Validate MAF data quality and structure for 546-day analysis.
        """
        analysis_columns = ['Buno', 'System Reason Description', 'WUC', 'Work Center']
        missing_cols = [col for col in analysis_columns if col not in df_maf.columns]
        
        if missing_cols:
            logger.warning(f"Missing MAF analysis columns: {missing_cols}")
        
        # Validate date columns availability
        date_columns = ['Received Date Time', 'Received Date', 'In Work Date', 'Comp Date Time', 'Comp Date']
        available_date_cols = [col for col in date_columns if col in df_maf.columns]
        logger.info(f"Available MAF date columns: {available_date_cols}")
        
        if 'WUC' in df_maf.columns and 'Work Center' in df_maf.columns:
            wuc_030000p_count = (df_maf['WUC'] == '030000P').sum()
            # Check both '020' and '20' formats
            wc_020_count = (df_maf['Work Center'] == '020').sum()
            wc_20_count = (df_maf['Work Center'] == '20').sum()
            logger.info(f"MAF data preview: {wuc_030000p_count} records with WUC '030000P'")
            logger.info(f"Work Center counts: '020'={wc_020_count}, '20'={wc_20_count}")
        
        logger.info(f"MAF data validation complete. Unique aircraft: {df_maf['Buno'].nunique()}")
    
    def calculate_fiscal_year(self, date) -> Optional[int]:
        """
        Calculate fiscal year for a given date with error handling.
        """
        try:
            if pd.isna(date):
                return None
                
            if isinstance(date, str):
                date = pd.to_datetime(date, errors='coerce')
            if isinstance(date, pd.Timestamp):
                date = date.to_pydatetime()
            elif not isinstance(date, datetime):
                date = pd.Timestamp(date).to_pydatetime()
            
            return date.year + 1 if date.month >= 10 else date.year
            
        except Exception as e:
            logger.warning(f"Error calculating fiscal year for date {date}: {e}")
            return None
    
    def calculate_fiscal_quarter(self, date) -> Optional[int]:
        """
        Calculate fiscal quarter for a given date with error handling.
        """
        try:
            if pd.isna(date):
                return None
                
            if isinstance(date, str):
                date = pd.to_datetime(date, errors='coerce')
            if isinstance(date, pd.Timestamp):
                date = date.to_pydatetime()
            elif not isinstance(date, datetime):
                date = pd.Timestamp(date).to_pydatetime()
            
            month = date.month
            
            if month >= 10:          # Oct, Nov, Dec
                return 1
            elif month <= 3:         # Jan, Feb, Mar
                return 2
            elif month <= 6:         # Apr, May, Jun
                return 3
            else:                    # Jul, Aug, Sep
                return 4
                
        except Exception as e:
            logger.warning(f"Error calculating fiscal quarter for date {date}: {e}")
            return None
    
    def process_depot_amarg_detection(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process depot data to identify AMARG aircraft and rebase events.
        """
        logger.info("Processing depot data for AMARG aircraft and rebase events...")
        
        # Initialize new columns with default values
        df['REBASE'] = False            
        df['546_EVENT'] = False         
        df['is_AMARG'] = False          
        df['Chart Visibility'] = True  
        
        for idx, row in df.iterrows():
            try:
                task_value = str(row.get('TASK', '')).upper()
                if 'REBASE' in task_value:
                    df.at[idx, 'REBASE'] = True
                
                sqd_value = str(row.get('SQD', '')).upper()
                if 'AMARG' in sqd_value:
                    df.at[idx, 'is_AMARG'] = True
                    
            except Exception as e:
                logger.warning(f"Error processing depot row {idx}: {e}")
                continue
        
        rebase_found = df['REBASE'].sum()
        amarg_found = df['is_AMARG'].sum()
        visible_records = df['Chart Visibility'].sum()
        logger.info(f"Depot data: Found {rebase_found} rebase events, {amarg_found} AMARG aircraft records")
        logger.info(f"Chart visibility: {visible_records} visible, {len(df) - visible_records} hidden")
        
        return df
    
    def analyze_maf_546_events(self, df_maf: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze MAF data to extract 546-day events with proper date preservation.
        
        Key: Keep MAF dates as strings throughout the entire process - no pandas conversion!
        """
        logger.info("="*60)
        logger.info("ANALYZING MAF 546-DAY EVENTS")
        logger.info("="*60)
        
        maf_546_findings = []
        complete_matches = 0
        date_issues = 0
        
        # Process each MAF record
        for idx, row in df_maf.iterrows():
            try:
                wuc_value = str(row.get('WUC', '')).strip()
                work_center_value = str(row.get('Work Center', '')).strip()
                system_reason = str(row.get('System Reason Description', '')).upper()
                
                # Apply 546-day event criteria
                wuc_match = (wuc_value == "030000P")        
                wc_match = (work_center_value == "020" or work_center_value == "20")
                reason_match = '546' in system_reason       
                
                if wuc_match and wc_match and reason_match:
                    complete_matches += 1
                    
                    # Keep dates as strings - no pandas conversion!
                    start_date_str = None
                    finish_date_str = None
                    date_source = None
                    
                    # Priority: Received Date Time > Received Date > In Work Date > Comp Date Time > Comp Date
                    date_columns_priority = [
                        ('Received Date Time', 'Received Date Time'),
                        ('Received Date', 'Received Date'), 
                        ('In Work Date', 'In Work Date'),
                        ('Comp Date Time', 'Comp Date Time'),
                        ('Comp Date', 'Comp Date')
                    ]
                    
                    for col_name, source_name in date_columns_priority:
                        if col_name in row and pd.notna(row[col_name]) and str(row[col_name]).strip():
                            raw_date = str(row[col_name]).strip()
                            
                            # Convert to clean YYYY-MM-DD format but keep as string
                            try:
                                # Parse the date but immediately convert to clean string format
                                parsed_date = pd.to_datetime(raw_date, errors='coerce')
                                if pd.notna(parsed_date):
                                    start_date_str = parsed_date.strftime('%Y-%m-%d')  # Clean string format
                                    date_source = source_name
                                    break
                            except:
                                continue
                    
                    # Calculate finish date as string
                    if start_date_str:
                        try:
                            start_parsed = pd.to_datetime(start_date_str)
                            finish_parsed = start_parsed + pd.Timedelta(days=DAY_546_OFFSET)
                            finish_date_str = finish_parsed.strftime('%Y-%m-%d')  # Clean string format
                        except:
                            finish_date_str = start_date_str  # Fallback
                    
                    if start_date_str:
                        # Calculate fiscal info using the string date
                        fiscal_year = self.calculate_fiscal_year(start_date_str)
                        fiscal_quarter = self.calculate_fiscal_quarter(start_date_str)
                        
                        # Create 546-day event record with STRING dates
                        maf_event = {
                            'BUNO': int(row.get('Buno', 0)) if pd.notna(row.get('Buno')) else None,
                            'START DATE': start_date_str,      # STRING - no Timestamp object!
                            'FINISH DATE': finish_date_str,    # STRING - no Timestamp object!
                            'TASK': '546DAY',
                            'data_source': 'maf_analysis',
                            
                            'WUC': wuc_value,
                            'Work_Center': work_center_value,
                            'System_Reason_Description': row.get('System Reason Description', ''),
                            
                            'JCN': row.get('Jcn', ''),      
                            'MCN': row.get('Mcn', ''),      
                            
                            'FY': fiscal_year,
                            'QTR': fiscal_quarter,
                            
                            'REBASE': 'REBASE' in system_reason,    
                            '546_EVENT': True,                      
                            'is_AMARG': False,                      
                            'Chart Visibility': True,               
                            
                            'date_source_used': date_source
                        }
                        
                        if maf_event['BUNO'] and maf_event['FY']:
                            maf_event['BUNO_key'] = f"{maf_event['BUNO']}_{maf_event['FY']}_{maf_event['TASK']}"
                        
                        maf_546_findings.append(maf_event)
                        
                        logger.info(f"✓ Created clean 546-day event: BUNO {maf_event['BUNO']}, Start: {start_date_str}, Finish: {finish_date_str}")
                        
                    else:
                        date_issues += 1
                        logger.warning(f"✗ 546-day event found but no valid date - BUNO: {row.get('Buno', 'Unknown')}")
                        
            except Exception as e:
                logger.error(f"Error analyzing MAF row {idx}: {e}")
                continue
        
        maf_546_df = pd.DataFrame(maf_546_findings)
        
        logger.info("="*60)
        logger.info("MAF ANALYSIS RESULTS")
        logger.info("="*60)
        logger.info(f"  Complete 546-day events found: {complete_matches:,}")
        logger.info(f"  Events with valid dates: {len(maf_546_df):,}")
        logger.info(f"  Date format: Clean YYYY-MM-DD strings (no timestamps)")
        
        return maf_546_df
    
    def analyze_bumblebee_slep_events(self, df_bb: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze BumbleBee ML data to generate SLEP events based on flight hours.
        """
        logger.info("Analyzing BumbleBee SLEP events...")
        
        if df_bb.empty:
            logger.warning("No BumbleBee data available for SLEP analysis")
            return pd.DataFrame()
        
        slep_findings = []
        
        # Statistics tracking
        slep_1_count = 0
        slep_2_count = 0
        slep_3_count = 0
        max_life_count = 0
        
        # Set SLEP scheduling start date to July 2020 as required
        base_slep_date = pd.Timestamp('2020-07-01')  # Start SLEP scheduling in July 2020
        
        slep_counter = 0  # Track SLEP events for date spacing
        
        for idx, row in df_bb.iterrows():
            try:
                buno = row.get('BUNO')
                flight_hours = row.get('AI_Running_Total_Flight_Hours', 0)
                
                if pd.isna(buno) or pd.isna(flight_hours):
                    continue
                
                # Determine SLEP level based on flight hours with updated durations
                if flight_hours >= SLEP_THRESHOLDS['MAX_LIFE']:
                    task = 'MAX_AIRCRAFT_LIFE'
                    max_life_count += 1
                    slep_duration = 180  # Updated: MAX_AIRCRAFT_LIFE = 180 days
                elif flight_hours >= SLEP_THRESHOLDS['SLEP_3']:
                    task = 'SLEP_3'
                    slep_3_count += 1
                    slep_duration = 90  # Updated: SLEP_3 = 90 days
                elif flight_hours >= SLEP_THRESHOLDS['SLEP_2']:
                    task = 'SLEP_2'  
                    slep_2_count += 1
                    slep_duration = 90  # Updated: SLEP_2 = 90 days
                elif flight_hours >= SLEP_THRESHOLDS['SLEP_1']:
                    task = 'SLEP_1'
                    slep_1_count += 1
                    slep_duration = 90  # Updated: SLEP_1 = 90 days
                else:
                    continue  # Skip aircraft below SLEP threshold
                
                # Calculate realistic SLEP scheduling dates
                # Space SLEP events 2 weeks apart for realistic scheduling
                start_date = base_slep_date + pd.Timedelta(weeks=slep_counter * 2)
                finish_date = start_date + pd.Timedelta(days=slep_duration)
                slep_counter += 1
                
                fiscal_year = self.calculate_fiscal_year(start_date)
                fiscal_quarter = self.calculate_fiscal_quarter(start_date)
                
                # Create SLEP event record with clean date strings
                slep_event = {
                    'BUNO': int(buno),
                    'START DATE': start_date.strftime('%Y-%m-%d'),     # Clean format
                    'FINISH DATE': finish_date.strftime('%Y-%m-%d'),   # Clean format
                    'TASK': task,
                    'data_source': 'BumbleBee',
                    
                    'AI_Flight_Hours': flight_hours,  # Flight hour data from BumbleBee
                    
                    'FY': fiscal_year,
                    'QTR': fiscal_quarter,
                    
                    'REBASE': False,
                    '546_EVENT': False,
                    'is_AMARG': False,
                    'Chart Visibility': True,
                }
                
                slep_event['BUNO_key'] = f"{buno}_{fiscal_year}_{task}"
                slep_findings.append(slep_event)
                
            except Exception as e:
                logger.error(f"Error analyzing BumbleBee row {idx}: {e}")
                continue
        
        slep_df = pd.DataFrame(slep_findings)
        
        logger.info("SLEP Analysis Results (Updated Durations):")
        logger.info(f"  SLEP 1 events: {slep_1_count:,} (90 days each)")
        logger.info(f"  SLEP 2 events: {slep_2_count:,} (90 days each)")
        logger.info(f"  SLEP 3 events: {slep_3_count:,} (90 days each)")
        logger.info(f"  Max life events: {max_life_count:,} (180 days each)")
        logger.info(f"  Total SLEP events created: {len(slep_df):,}")
        
        return slep_df
    
    def generate_maf_546_projections(self, df_maf: pd.DataFrame) -> pd.DataFrame:
        """
        Generate future 546-day projections based on MAF data patterns.
        """
        logger.info("Generating MAF 546-day projections...")
        
        if df_maf.empty:
            logger.warning("No MAF data available for 546-day projections")
            return pd.DataFrame()
        
        projection_findings = []
        
        # Find aircraft with historical 546-day events
        historical_546_aircraft = set()
        
        for idx, row in df_maf.iterrows():
            try:
                wuc_value = str(row.get('WUC', '')).strip()
                work_center_value = str(row.get('Work Center', '')).strip()
                system_reason = str(row.get('System Reason Description', '')).upper()
                
                # Check for 546-day criteria
                wuc_match = (wuc_value == "030000P")
                wc_match = (work_center_value == "020" or work_center_value == "20")
                reason_match = '546' in system_reason
                
                if wuc_match and wc_match and reason_match:
                    buno = row.get('Buno')
                    if pd.notna(buno):
                        historical_546_aircraft.add(int(buno))
                        
            except Exception as e:
                continue
        
        logger.info(f"Found {len(historical_546_aircraft)} aircraft with historical 546-day events")
        
        # Generate projections for each aircraft
        current_date = pd.Timestamp.now()
        end_date = pd.Timestamp(datetime(PROJECTION_END_YEAR, 12, 31))
        
        for buno in historical_546_aircraft:
            try:
                # Start projections from current date
                projection_date = current_date
                
                while projection_date < end_date:
                    projection_date = projection_date + pd.Timedelta(days=DAY_546_INTERVAL)
                    
                    if projection_date >= end_date:
                        break
                    
                    fiscal_year = self.calculate_fiscal_year(projection_date)
                    fiscal_quarter = self.calculate_fiscal_quarter(projection_date)
                    
                    # Create projection record with clean date strings
                    projection_event = {
                        'BUNO': buno,
                        'START DATE': projection_date.strftime('%Y-%m-%d'),
                        'FINISH DATE': (projection_date + pd.Timedelta(days=DAY_546_OFFSET)).strftime('%Y-%m-%d'),
                        'TASK': '546DAY_PROJ',
                        'data_source': 'maf_projection',
                        
                        'FY': fiscal_year,
                        'QTR': fiscal_quarter,
                        
                        'REBASE': False,
                        '546_EVENT': True,
                        'is_AMARG': False,
                        'Chart Visibility': True,
                    }
                    
                    projection_event['BUNO_key'] = f"{buno}_{fiscal_year}_546DAY_PROJ"
                    projection_findings.append(projection_event)
                    
            except Exception as e:
                logger.error(f"Error generating 546-day projections for BUNO {buno}: {e}")
                continue
        
        projection_df = pd.DataFrame(projection_findings)
        logger.info(f"Total projection events created: {len(projection_df):,}")
        
        return projection_df
    
    def get_tms_for_buno(self, buno: str, buno_df: pd.DataFrame) -> Optional[str]:
        """Get the most recent TMS (Type/Model/Series) for a specific aircraft."""
        tms_columns = [col for col in buno_df.columns if 'TMS' in col.upper()]
        
        if not tms_columns:
            return None
        
        tms_col = tms_columns[0]
        valid_tms = buno_df[pd.notna(buno_df[tms_col])].sort_values('START DATE', ascending=False)
        
        return valid_tms[tms_col].iloc[0] if not valid_tms.empty else None
    
    def generate_future_events_enhanced(self, buno: str, buno_df: pd.DataFrame) -> List[Dict]:
        """Enhanced future event generation with comprehensive logic and error handling."""
        future_events = []
        
        try:
            # Find the most recent PMI task
            last_pmi_task = self.find_last_pmi_task(buno_df)
            if not last_pmi_task:
                logger.warning(f"No PMI task found for BUNO {buno}")
                return future_events
            
            # Get aircraft type information
            tms = self.get_tms_for_buno(buno, buno_df)
            
            # Determine next PMI task
            next_pmi_task = self.get_next_pmi_task(last_pmi_task)
            
            # Find the most recent date
            last_date = self.get_most_recent_date(buno_df)
            if not last_date:
                logger.warning(f"No valid dates found for BUNO {buno}")
                return future_events
            
            # Get squadron assignment
            last_sqd = self.get_last_squadron(buno_df, last_date)
            
            # Skip AMARG aircraft for future PMI projections (but keep them visible in charts)
            if last_sqd and 'AMARG' in str(last_sqd).upper():
                logger.info(f"Skipping PMI projections for AMARG aircraft {buno}")
                return future_events
            
            if buno_df['is_AMARG'].any():
                logger.info(f"Skipping PMI projections for AMARG aircraft {buno} (marked in data)")
                return future_events
            
            # Generate future events
            current_date = last_date
            current_task = next_pmi_task
            end_date = pd.Timestamp(datetime(PROJECTION_END_YEAR, 12, 31))
            
            iteration_count = 0
            max_iterations = 100
            
            while current_date < end_date and iteration_count < max_iterations:
                iteration_count += 1
                
                # Calculate interval
                if '1' in str(current_task):
                    interval = PMI_1_2_INTERVAL
                else:
                    interval = PMI_2_1_INTERVAL
                
                # Calculate next date
                next_date = current_date + pd.DateOffset(months=interval)
                
                if next_date >= end_date:
                    break
                
                # Create finish date for future events
                finish_date = next_date + pd.Timedelta(days=DAY_546_OFFSET)
                
                # Create future maintenance event with clean date strings
                future_events.append({
                    'BUNO': buno,
                    'TMS': tms,
                    'START DATE': next_date.strftime('%Y-%m-%d'),
                    'FINISH DATE': finish_date.strftime('%Y-%m-%d'),
                    'SQD': None,
                    'TASK': current_task,
                    'FY': self.calculate_fiscal_year(next_date),
                    'QTR': self.calculate_fiscal_quarter(next_date),
                    'data_source': 'depot_projection',
                    'REBASE': False,
                    '546_EVENT': False,
                    'is_AMARG': False,
                    'Chart Visibility': True
                })
                
                # Alternate task type
                current_task = self.get_next_pmi_task(current_task)
                current_date = next_date
            
        except Exception as e:
            logger.error(f"Error generating future events for BUNO {buno}: {e}")
        
        return future_events
    
    def find_last_pmi_task(self, buno_df: pd.DataFrame) -> Optional[str]:
        """Find the most recent PMI task for a specific aircraft."""
        for _, row in buno_df.sort_values('START DATE', ascending=False).iterrows():
            if pd.notna(row.get('TASK')):
                task = str(row['TASK']).upper()
                if 'PMI' in task:
                    return row['TASK']
        return None
    
    def get_next_pmi_task(self, current_task: str) -> str:
        """Determine the next PMI task in the alternation sequence."""
        task_upper = current_task.upper()
        
        if '1' in task_upper:
            return current_task.replace('1', '2')
        else:
            return current_task.replace('2', '1')
    
    def get_most_recent_date(self, buno_df: pd.DataFrame) -> Optional[pd.Timestamp]:
        """Get the most recent maintenance date for a specific aircraft."""
        valid_start_dates = buno_df[pd.notna(buno_df['START DATE'])]['START DATE']
        
        finish_col = 'FINISH DATE' if 'FINISH DATE' in buno_df.columns else None
        valid_finish_dates = buno_df[pd.notna(buno_df[finish_col])][finish_col] if finish_col else pd.Series(dtype='datetime64[ns]')
        
        if not valid_start_dates.empty and not valid_finish_dates.empty:
            return max(valid_start_dates.max(), valid_finish_dates.max())
        elif not valid_start_dates.empty:
            return valid_start_dates.max()
        elif not valid_finish_dates.empty:
            return valid_finish_dates.max()
        else:
            return None
    
    def get_last_squadron(self, buno_df: pd.DataFrame, last_date: pd.Timestamp) -> Optional[str]:
        """Get the most recent squadron assignment for a specific aircraft."""
        last_sqd_records = buno_df.loc[buno_df['START DATE'] == last_date, 'SQD']
        if not last_sqd_records.empty and pd.notna(last_sqd_records.iloc[0]):
            return last_sqd_records.iloc[0]
        
        valid_sqd_records = buno_df[pd.notna(buno_df['SQD'])].sort_values('START DATE', ascending=False)
        return valid_sqd_records['SQD'].iloc[0] if not valid_sqd_records.empty else None
    
    def add_enhanced_buno_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create enhanced BUNO key with duplicate detection and resolution."""
        df['BUNO_key'] = df['BUNO'].astype(str) + "_" + df['FY'].astype(str) + "_" + df['TASK'].astype(str)
        
        duplicate_mask = df.duplicated(subset=['BUNO_key'], keep=False)
        if duplicate_mask.any():
            logger.warning(f"Found {duplicate_mask.sum()} duplicate BUNO_key values")
            
            df['sequence'] = df.groupby('BUNO_key').cumcount()
            df.loc[duplicate_mask, 'BUNO_key'] = (
                df.loc[duplicate_mask, 'BUNO_key'] + "_" + 
                df.loc[duplicate_mask, 'sequence'].astype(str)
            )
            df.drop('sequence', axis=1, inplace=True)
        
        # Reorder columns to place BUNO_key after BUNO
        cols = df.columns.tolist()
        if 'BUNO' in cols and 'BUNO_key' in cols:
            buno_index = cols.index('BUNO')
            cols.remove('BUNO_key')
            cols.insert(buno_index + 1, 'BUNO_key')
            df = df[cols]
        
        return df
    
    def calculate_enhanced_deltas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate enhanced time delta columns with comprehensive error handling."""
        logger.info("Calculating delta columns...")
        
        date_columns = {
            'FID': 'FID',
            'START DATE': 'START DATE',
            'FINISH DATE': 'FINISH DATE'
        }
        
        # Convert date columns
        for col_name, col_key in date_columns.items():
            if col_key in df.columns:
                df[col_key] = pd.to_datetime(df[col_key], errors='coerce')
        
        # Calculate deltas
        if 'FID' in df.columns and 'START DATE' in df.columns:
            df['FID_START_DATE_DELTA'] = (df['START DATE'] - df['FID']).dt.days
        
        if 'START DATE' in df.columns and 'FINISH DATE' in df.columns:
            df['START_FINISH_DATE_DELTA'] = (df['FINISH DATE'] - df['START DATE']).dt.days
        
        return df
    
    def export_data(self, df: pd.DataFrame, file_key: str, description: str):
        """
        Export DataFrame to CSV with clean file naming.
        """
        try:
            if file_key in self.output_paths:
                file_path = self.output_paths[file_key]
            else:
                filename = f"{file_key}.csv" if not file_key.endswith('.csv') else file_key
                file_path = os.path.join(self.version_folder, filename)
            
            # Simple export - no special date handling needed since dates are already clean strings
            df_export = df.copy()
            
            # Clean up boolean columns
            boolean_columns = ['REBASE', '546_EVENT', 'is_AMARG', 'Chart Visibility']
            for col in boolean_columns:
                if col in df_export.columns:
                    df_export[col] = df_export[col].astype(bool)
            
            # Simple CSV export - dates are already in proper string format
            df_export.to_csv(file_path, index=False)
            logger.info(f"✓ Successfully exported {len(df)} {description} records to {file_path}")
            
            # Log summary
            if '546_EVENT' in df.columns:
                event_546_count = df['546_EVENT'].sum()
                logger.info(f"  -> 546_EVENT records in export: {event_546_count}")
            
            if 'data_source' in df.columns:
                source_counts = df['data_source'].value_counts()
                logger.info(f"  -> Data sources: {dict(source_counts)}")
                
        except Exception as e:
            logger.error(f"Error exporting {description}: {e}")
    
    def generate_summary_report(self) -> Dict:
        """Generate comprehensive summary report of all processed data."""
        summary = {
            'total_records': len(self.combined_df),
            'historical_depot_records': len(self.df),
            'projected_records': len(self.future_df),
            'maf_546_events': len(self.maf_546_events) if self.maf_546_events is not None else 0,
            'maf_546_projections': len(self.maf_546_projections) if self.maf_546_projections is not None else 0,
            'slep_events': len(self.slep_events) if self.slep_events is not None else 0,
            'unique_aircraft': self.combined_df['BUNO'].nunique(),
            
            'date_range': {
                'earliest': self.combined_df['START DATE'].min(),
                'latest': self.combined_df['START DATE'].max()
            },
            
            'task_distribution': self.combined_df['TASK'].value_counts().to_dict(),
            'data_source_distribution': self.combined_df['data_source'].value_counts().to_dict(),
            
            'rebase_events': self.combined_df['REBASE'].sum() if 'REBASE' in self.combined_df.columns else 0,
            '546_events': self.combined_df['546_EVENT'].sum() if '546_EVENT' in self.combined_df.columns else 0,
            'amarg_aircraft': self.combined_df['is_AMARG'].sum() if 'is_AMARG' in self.combined_df.columns else 0,
            'chart_visible_records': self.combined_df['Chart Visibility'].sum() if 'Chart Visibility' in self.combined_df.columns else 0,
            'chart_hidden_records': (~self.combined_df['Chart Visibility']).sum() if 'Chart Visibility' in self.combined_df.columns else 0,
        }
        
        return summary
    
    def run_processing(self):
        """Main processing pipeline for aircraft maintenance scheduling with comprehensive updates."""
        logger.info("Starting comprehensive aircraft maintenance scheduling processing (v7.1)...")
        
        # STEP 1: Load depot data
        logger.info("Step 1: Loading depot data...")
        self.df = self.load_depot_data()
        self.df['START DATE'] = pd.to_datetime(self.df['START DATE'])
        self.df['data_source'] = 'H60_IMP_Report'
        
        # STEP 2: Load and analyze MAF data
        logger.info("Step 2: Loading and analyzing MAF data...")
        self.df_maf = self.load_maf_data()
        self.maf_546_events = self.analyze_maf_546_events(self.df_maf)
        
        # STEP 3: Load and analyze BumbleBee data
        logger.info("Step 3: Loading and analyzing BumbleBee ML data...")
        self.df_bumblebee = self.load_bumblebee_data()
        self.slep_events = self.analyze_bumblebee_slep_events(self.df_bumblebee)
        
        # STEP 4: Generate MAF 546-day projections
        logger.info("Step 4: Generating MAF 546-day projections...")
        self.maf_546_projections = self.generate_maf_546_projections(self.df_maf)
        
        # STEP 5: Process depot data for AMARG aircraft and rebase events
        logger.info("Step 5: Processing depot data for AMARG and rebase events...")
        self.df = self.process_depot_amarg_detection(self.df)
        
        # STEP 6: Generate future PMI events
        logger.info("Step 6: Generating future PMI maintenance events...")
        all_future_events = []
        unique_bunos = self.df['BUNO'].unique()
        
        for i, buno in enumerate(unique_bunos, 1):
            if i % 50 == 0:
                logger.info(f"Processing aircraft {i}/{len(unique_bunos)}")
                
            buno_df = self.df[self.df['BUNO'] == buno].copy()
            future_events = self.generate_future_events_enhanced(buno, buno_df)
            all_future_events.extend(future_events)
        
        # STEP 7: Create future events dataframe
        self.future_df = pd.DataFrame(all_future_events)
        if not self.future_df.empty:
            self.future_df['data_source'] = 'depot_projection'
        logger.info(f"Generated {len(self.future_df)} total future PMI events")
        
        # STEP 8: Harmonize all dataframes
        logger.info("Step 8: Harmonizing data structures...")
        all_dataframes = [self.df, self.future_df]
        
        if self.maf_546_events is not None and not self.maf_546_events.empty:
            all_dataframes.append(self.maf_546_events)
        
        if self.maf_546_projections is not None and not self.maf_546_projections.empty:
            all_dataframes.append(self.maf_546_projections)
            
        if self.slep_events is not None and not self.slep_events.empty:
            all_dataframes.append(self.slep_events)
        
        # Get all unique columns across all dataframes
        all_columns = set()
        for df in all_dataframes:
            if not df.empty:
                all_columns.update(df.columns)
        
        # Add missing columns to each dataframe
        for df in all_dataframes:
            if not df.empty:
                for col in all_columns:
                    if col not in df.columns:
                        df[col] = None
        
        # STEP 9: Calculate fiscal information for all dataframes
        logger.info("Step 9: Calculating fiscal years and quarters...")
        for dataframe in all_dataframes:
            if not dataframe.empty:
                if 'QTR' not in dataframe.columns or dataframe['QTR'].isna().all():
                    dataframe['QTR'] = dataframe['START DATE'].apply(self.calculate_fiscal_quarter)
                if 'FY' not in dataframe.columns or dataframe['FY'].isna().all():
                    dataframe['FY'] = dataframe['START DATE'].apply(self.calculate_fiscal_year)
        
        # STEP 10: Add unique keys for all dataframes
        logger.info("Step 10: Adding unique keys...")
        if not self.df.empty:
            self.df = self.add_enhanced_buno_key(self.df)
        if not self.future_df.empty:
            self.future_df = self.add_enhanced_buno_key(self.future_df)
        if self.maf_546_events is not None and not self.maf_546_events.empty:
            self.maf_546_events = self.add_enhanced_buno_key(self.maf_546_events)
        if self.maf_546_projections is not None and not self.maf_546_projections.empty:
            self.maf_546_projections = self.add_enhanced_buno_key(self.maf_546_projections)
        if self.slep_events is not None and not self.slep_events.empty:
            self.slep_events = self.add_enhanced_buno_key(self.slep_events)
        
        # STEP 11: Combine all data sources
        logger.info("Step 11: Combining all data sources...")
        
        # Start with depot data
        logger.info(f"Depot data shape: {self.df.shape}")
        self.combined_df = self.df.copy()
        
        # Add future PMI events
        if not self.future_df.empty:
            logger.info(f"Adding future PMI events: {len(self.future_df)}")
            self.combined_df = pd.concat([self.combined_df, self.future_df], ignore_index=True)
        
        # Add MAF 546-day events (dates are already clean strings)
        if self.maf_546_events is not None and not self.maf_546_events.empty:
            logger.info(f"Adding MAF 546-day events: {len(self.maf_546_events)}")
            # No date conversion needed - dates are already clean strings
            self.combined_df = pd.concat([self.combined_df, self.maf_546_events], ignore_index=True)
        
        # Add MAF 546-day projections
        if self.maf_546_projections is not None and not self.maf_546_projections.empty:
            logger.info(f"Adding MAF 546-day projections: {len(self.maf_546_projections)}")
            self.combined_df = pd.concat([self.combined_df, self.maf_546_projections], ignore_index=True)
        
        # Add SLEP events
        if self.slep_events is not None and not self.slep_events.empty:
            logger.info(f"Adding SLEP events: {len(self.slep_events)}")
            self.combined_df = pd.concat([self.combined_df, self.slep_events], ignore_index=True)
        
        logger.info(f"Final combined dataset shape: {self.combined_df.shape}")
        
        # STEP 12: Calculate time deltas
        logger.info("Step 12: Calculating time deltas...")
        self.combined_df = self.calculate_enhanced_deltas(self.combined_df)
        
        # STEP 13: Sort data
        logger.info("Step 13: Sorting final data...")
        self.combined_df = self.combined_df.sort_values(['BUNO', 'START DATE'])
        
        # STEP 14: Export all data files
        logger.info("Step 14: Exporting data files to version folder...")
        
        # Export main combined dataset
        self.export_data(self.combined_df, 'combined', "combined comprehensive dataset")
        
        # Export future PMI events
        if not self.future_df.empty:
            self.export_data(self.future_df, 'future', "future PMI events")
        
        # Export MAF 546-day analysis
        if self.maf_546_events is not None and not self.maf_546_events.empty:
            self.export_data(self.maf_546_events, 'maf_analysis', "MAF 546-day analysis")
        
        # Export MAF 546-day projections
        if self.maf_546_projections is not None and not self.maf_546_projections.empty:
            self.export_data(self.maf_546_projections, 'maf_projection', "MAF 546-day projections")
        
        # Export SLEP events
        if self.slep_events is not None and not self.slep_events.empty:
            self.export_data(self.slep_events, 'slep_analysis', "SLEP events")
        
        # Export rebase events
        if 'REBASE' in self.combined_df.columns:
            rebase_df = self.combined_df[self.combined_df['REBASE'] == True]
            if not rebase_df.empty:
                self.export_data(rebase_df, 'rebase', "rebase events")
        
        # STEP 15: Generate comprehensive summary
        logger.info("Step 15: Generating comprehensive summary report...")
        summary = self.generate_summary_report()
        
        logger.info("="*60)
        logger.info("COMPREHENSIVE PROCESSING SUMMARY (v7.1)")
        logger.info("="*60)
        logger.info(f"  Total combined records: {summary['total_records']:,}")
        logger.info(f"  Historical depot records: {summary['historical_depot_records']:,}")
        logger.info(f"  Projected PMI records: {summary['projected_records']:,}")
        logger.info(f"  MAF 546-day events: {summary['maf_546_events']:,}")
        logger.info(f"  MAF 546-day projections: {summary['maf_546_projections']:,}")
        logger.info(f"  SLEP events: {summary['slep_events']:,}")
        logger.info(f"  Unique aircraft: {summary['unique_aircraft']:,}")
        logger.info(f"  Date range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
        logger.info(f"  Rebase events: {summary['rebase_events']:,}")
        logger.info(f"  546-day events: {summary['546_events']:,}")
        logger.info(f"  AMARG aircraft: {summary['amarg_aircraft']:,}")
        logger.info(f"  Chart visible records: {summary['chart_visible_records']:,}")
        logger.info(f"  Chart hidden records: {summary['chart_hidden_records']:,}")
        
        # Data source breakdown
        logger.info("\nData Source Distribution:")
        for source, count in summary['data_source_distribution'].items():
            logger.info(f"  {source}: {count:,}")
        
        logger.info("="*60)
        logger.info("v7.1: Aircraft depot scheduling processing completed successfully!")
        logger.info(f"All output files saved to: {self.version_folder}")
        logger.info("="*60)
        logger.info("REFINEMENTS APPLIED (Version 7.1):")
        logger.info("✓ Set SLEP scheduling start date to July 2020 as required")
        logger.info("✓ Updated SLEP durations: SLEP 1-3 = 90 days, MAX_AIRCRAFT_LIFE = 180 days")
        logger.info("✓ MAF dates now appear as clean YYYY-MM-DD format (no timestamps)")
        logger.info("✓ Clean output file names ending with version only (no UPDATED/FIXED suffixes)")
        logger.info("✓ All V7 functionality preserved (BumbleBee, MAF projections, etc.)")
        logger.info("="*60)


###---------------------------------MAIN EXECUTION SECTION---------------------------------###

if __name__ == "__main__":
    """
    Main execution block - runs when script is executed directly.
    
    Exit codes:
    - 0: Success
    - 1: Fatal error occurred
    """
    try:
        scheduler = AircraftScheduler()
        scheduler.run_processing()
        logger.info("Script v7.1 completed successfully with all refinements applied!")
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Fatal error occurred: {e}")
        logger.error("Check the log file for detailed error information")
        sys.exit(1)