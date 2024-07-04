import json
import os
import sys
import time

import schedule
import pandas as pd

sys.path.append(os.getcwd())
from data_extractor import DataExtractor
from utils import separate_into_breakpoints


with open('configs.json', 'r') as f:
    CONFIGS = json.load(f)

def main():

    extractor = DataExtractor(CONFIGS)

    t0 = time.time()
    
    extractor.logger.info(f'Extraction started on {t0}')

    # find break points to avoid exceeding OWM's api quota
    student_ids = list(extractor.students_dict.keys())
    breakpoints = separate_into_breakpoints(len(student_ids), step=CONFIGS['main']['breakpoint_steps'])
    
    extractor.logger.info("Begin extraction & AQI calculations")
    dfs_rt = []
    dfs_fc = []
    for i in range(len(breakpoints)-1):
        if len(student_ids) <= CONFIGS['main']['breakpoint_steps']:
            student_chunks = student_ids
        else:
            student_chunks = student_ids[breakpoints[i]:breakpoints[i+1]]
        dfs_rt.append(extractor.get_rt(student_chunks))
        dfs_fc.append(extractor.get_fc(student_chunks))
        if len(student_ids) > CONFIGS['main']['breakpoint_steps']:
            time.sleep(60)

    extractor.logger.info("Concatenating DataFrames")
    rt = pd.concat(dfs_rt, axis=0).reset_index(drop=True)
    fc = pd.concat(dfs_fc, axis=0).reset_index(drop=True)

    full = pd.merge(rt, fc, on=['STUDENT_ID'], how='outer')

    df_insert = full[['STUDENT_ID', 'LAT', 'LON', 'DT', 'AQI_CURRENT',
                      'AQI_TODAY', 'AQI_NEXT_DAY']]

    
    try:
        df_insert.to_sql('daily_mycap', con=extractor.sqlach_engine, if_exists='append', index=False)
        extractor.logger.info('Successfully inserted data into Database')
    except Exception as exp:
        extractor.logger.exception("Exception encountered: ", exp)

    extractor.db.commit()

    t1 = time.time()
    extractor.logger.info(f'Could not retrieve current AQI for {extractor.rt_error_list}')
    extractor.logger.info(f'Could not retrieve current AQI for {extractor.fc_error_list}')
    extractor.logger.info(f'Extraction ended at {t1}')
    extractor.logger.info(f'Execution time: {t1 - t0}')
    


if __name__ == '__main__':

    main()
    
    # schedule.every().day.at(CONFIGS['main']['job_time'], CONFIGS['main']['timezone']).do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
        

        