##pandas file profilier functions
import pandas as pd
from mb_utils.src.logging import logger

__all__ = ['create_profile','profile_compare']

def create_profile(df,profile_name='./pandas_profiling_report.html',minimal=False,target=[],logger=None):
    """
    Create pandas profiling report
    Input:
        df (pd.DataFrame): pandas dataframe
        profile_name (str): profile name -location. Default : ./pandas_profiling_report.html
        minimal (bool): minimal profile
        target (list): target columns
    Output:
        None
    """
    if len(df) > 100000:
        if logger:
            logger.warning('Dataframe is too large for profiling')
            logger.info('Setting sample size to 100000')
        df = df.sample(100000)
        minimal = True
        if logger:
            logger.info('Creating profile minimal report')
    from pandas_profiling import ProfileReport
    if logger:
        logger.info('Creating profile report with all features, len(df) < 100000')
    profile = ProfileReport(df,title='Pandas Profiling Report', minimal=minimal, html={'style':{'full_width':True}})
    if len(target)>0:
        if logger:
            logger.info('Creating profile report with target features')
            logger.info(f'tagets: {target}')
            assert all([t in df.columns for t in target]), logger.info('Target column not in dataframe')
        profile.config.interactions.targets = target
    #else:
    #    profile.config.interactions.targets = df.columns
    profile.to_file(output_file=profile_name)
    if logger:
        logger.info(f'Pandas profiling report created: {profile_name}')
    
def profile_compare(df1,df2,profile_name='./pandas_compare_report.html',logger=None):
    """
    Creating pandas comparison report
    Input:
        df1 (pd.DataFrame): pandas dataframe
        df2 (pd.DataFrame): pandas dataframe
        profile_name (str): profile name -location. Default : ./pandas_compare_report.html
    Output:
        None
    """
    df1_file = create_profile(df1,profile_name='./df1_pandas_profiling_report.html',minimal=False,logger=logger)
    df2_file = create_profile(df2,profile_name='./df2_pandas_profiling_report.html',minimal=False,logger=logger)
    if logger:
        logger.info('Creating comparison report')
    df_compare = df1_file.compare(df2_file)
    df_compare.to_file(profile_name)
    if logger:
        logger.info(f'Pandas comparison report created: {profile_name}')
    
