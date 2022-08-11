## OPEN API를 사용하여 수질데이터 요청(Json파일로 부호화)
## 요청한 데이터를 mongoDB에 업로드
## 요청한 데이터 처리 및 부호화(pickle)

from flask_app.database.into_mongoDB import get_water_data
import pandas as pd
from pandas import json_normalize
import numpy as np
import pickle, os
from flask_app.model.model import feat_engi2, target_split, true_false_comp
from flask_app import pipe_FILEPATH, df_p_FILEPATH

## json to dataframe
def data_processing(w_year='2022', w_mon='01', pageNo='1', numOfRows='1'):
    df_json = get_water_data(w_year, w_mon, pageNo, numOfRows)
    df_json_sum = []

    for i in range(len(df_json)):
        df_json_sum.extend(df_json[f'{i}']['getWaterMeasuringList']['item'])
    df = json_normalize(df_json_sum)

    ## 위/경도 도분초 => 도 변경
    def Lat_Long_tran(LAT='LAT'):
        LAT_SEC = LAT + '_SEC'
        LAT_MIN = LAT + '_MIN'
        LAT_DGR = LAT + '_DGR'

        df[LAT_SEC] = df[LAT_SEC].fillna(0)
        df[LAT_MIN] = df[LAT_MIN].fillna(0)
        df2= df['LAT_DGR'].copy()
        for i in range(len(df[LAT_DGR])):
            if not df2[i]:
                df2[i] = np.nan
            else :
                df2[i] = df[LAT_DGR][i] + df[LAT_MIN][i]/60 + df[LAT_SEC][i]/3600
        return df2

    ##실행        
    df['LAT'] = Lat_Long_tran(LAT='LAT')
    df['LON'] = Lat_Long_tran(LAT='LON')


    ## 컬럼 위치 및 이름 변경
    df = df[['WMYR', 'WMOD', 'WMWK', 'WMCYMD', 'PT_NO', 'PT_NM', 'LAT', 'LON', 'ITEM_SS', 
            'ITEM_BOD', 'ITEM_PH', 'ITEM_TEMP', 'ITEM_NH3N', 'ITEM_DOC',
            'ITEM_DTP', 'ITEM_DTN', 'ITEM_AMNT', 'ITEM_POP', 'ITEM_EC',
            'ITEM_NO3N', 'ITEM_TOC', 'ITEM_TP', 'ITEM_TN', 
            'ITEM_CLOA', 'ITEM_COD']]
    df.columns = ['년도', '월', '회차', '검사일자', '수질측정망_코드', '수질측정망_명', '위도', '경도', '부유물질(SS)',
                '생물학적산소요구량(BOD)', '수소이온농도(pH)', '수온', '암모니아성질소(NH₃-N)', '용존산소(DO)',
                '용존총인(DTP)', '용존총질소(DTN)', '유량', '인산염(PO₄-P)', '전기전도도(EC)',
                '질산성질소(NO₃-N)', '총유기탄소(TOC)', '총인(T-P)', '총질소(T-N)',
                '클로로필-a(Chlorophyll-a)', '화학적산소요구량(COD)']


    ## 문자/특수문자 제거 및 타입변환
    df['검사일자'] = df['검사일자'].str.replace('.', '')
    df['회차'] = df['회차'].str.replace('회차', '')
    for n in df.columns:
        if (n != '수질측정망_코드') and (n != '수질측정망_명') :
            df[n] = pd.to_numeric(df[n], errors='coerce')


    ## feature engineering & test target split
    df = feat_engi2(df)
    X_test, y_test = target_split(df)

    
    ## pipe 피클링
    pipe = None
    with open(pipe_FILEPATH,'rb') as pickle_file:
        pipe = pickle.load(pickle_file)
    df_p = true_false_comp(pipe, X_test, y_test)


    ## 데이터 피클링
    #df_p_FILEPATH = os.path.join(os.getcwd(), 'flask_app\\database\\DB\\', 'df_p_data.pkl') 
    with open(df_p_FILEPATH,'wb') as pickle_file:
        pickle.dump(df_p, pickle_file)
    
    return df_p