import numpy as np
from tqdm import tqdm
import zipfile
import datetime
import os
import pandas as pd

"""==============================================
                   CONFIG VALUES
   ==============================================
"""

# ! READ ME !
# LINK_CSV_DIR 위치를 설정해주세요.
# 교통속도 값만을 얻고자 한다면 FILTERED_CSV_DIR 위치만 수정해주세요.
# ITS 에서 받는 압축파일을 압축해제 -> 필터링 -> 저장 하려면 ORIGINAL_CSV_DIR 위치만 수정해주세요.

LINK_CSV_DIR = "link_seoul_ver3.csv"  # LINK ID CSV 파일의 위치 (link_seoul_ver3.csv)

start_yy = 2020
start_mm = 1
start_dd = 1

end_yy = 2021
end_mm = 12
end_dd = 31

""" =============================================
    =============================================
"""

columns = ['DATE', 'LINK_ID', 'TRAFFIC_SPD']


class CreateSPD_Dataset:

    # ORIGINAL_CSV_DIR : ITS 에서 받은 365 일 * 2년 개의 CSV 압축파일이 들어있는 폴더 경로 (마지막 / 포함)
    # FILTERED_CSV_DIR : ITS 에서 받는 데이터를 필터링한 CSV 파일들이 들이었는 폴도 경로
    def __init__(self, ORIGINAL_CSV_DIR, FILTERED_CSV_DIR):
        self.ORIGINAL_CSV_DIR = ORIGINAL_CSV_DIR
        self.FILTERED_CSV_DIR = FILTERED_CSV_DIR
        self.main()

    def getLinkList(self):
        print("Seoul link ver3 DIR : ", LINK_CSV_DIR)
        link_data = pd.read_csv(LINK_CSV_DIR, encoding='cp949')

        link_list = link_data['LINK_ID'].to_numpy()
        print(len(link_list), "Link Loaded!")
        return link_list

    def unZip(self, file_dir, file_name):
        tqdm.write(f'Try to Unzip File : {file_name}')
        if os.path.exists(file_dir) is False:
            tqdm.write(f"No such file in {file_dir}")
            return False

        zf = zipfile.ZipFile(file_dir)
        zf.extractall(self.ORIGINAL_CSV_DIR)
        zf.close()
        tqdm.write("UnZip Done.\n")
        return True

    def link_filter(self, file_dir):
        tqdm.write("Try to Filtering Link ID")
        df = pd.read_csv(file_dir, header=None, usecols=[0, 1, 2], names=columns)
        df = df.loc[(df.iloc[:, 1].isin(self.link_list))]
        tqdm.write("Filtering Done.\n")
        return df

    def removeFile(self, file_dir):
        if os.path.isfile(file_dir):
            os.remove(file_dir)
        else:
            tqdm.write("\n[ERROR] 파일이 존재하지 않습니다.")
            tqdm.write(f"삭제 시도한 파일 : {file_dir}\n")

    def main(self):
        curr_date = datetime.datetime(start_yy, start_mm, start_dd)
        end_date = datetime.datetime(end_yy, end_mm, end_dd)

        print("============== [ ITS OpenAPI ] =============")
        print("")
        print("시작 날짜 : ", curr_date)
        print("종료 날짜 : ", end_date)
        print("")
        print("파일 저장 위치 : ", self.ORIGINAL_CSV_DIR)
        print("")
        self.link_list = self.getLinkList()
        print("")

        day_diff = end_date - curr_date
        day_diff = day_diff.days + 1
        print(f" DAY DIFF : {day_diff}")
        print("")
        print("============================================\n")

        # 20221022_5Min.zip
        for _ in tqdm(range(0, day_diff), leave=False):
            tqdm.write("")
            str_date = curr_date.strftime("%Y%m%d")
            file_zip_name = str_date + "_5Min.zip"
            file_csv_name = str_date + "_5Min.csv"

            result = self.unZip(file_dir=(self.ORIGINAL_CSV_DIR + file_zip_name), file_name=str_date)
            if result is False:
                continue

            df = self.link_filter(file_dir=(self.ORIGINAL_CSV_DIR + file_csv_name))
            df.to_csv((self.FILTERED_CSV_DIR + file_csv_name), encoding='utf8', index=False)
            self.removeFile(file_dir=(self.ORIGINAL_CSV_DIR + file_csv_name))
            curr_date += datetime.timedelta(days=1)


class TrafficSpdMgr:
    '''
    필터링된 교통속도 csv 파일들이 들어있는 폴더로 부터
    원하는 시각 및 LINK ID에 대한 속도값을 얻기 위한 클래스입니다.

    사용 전, TrafficSpeed.py 최상단에서 Config 값들을 수정해 주세요.

    TrafficSpdMgr 객체를 생성한 후 getSpeed 함수를 호출하여
    원하는 시각 및 LINK ID 에 따른 속도값을 얻을 수 있습니다.
    '''

    def __init__(self, FILTERED_CSV_DIR):
        if os.path.isdir(FILTERED_CSV_DIR) is False:
            print("[ TrafficSpeed.py ][ ERROR ] 교통 속도 데이터 CSV 파일들이 위치한 폴더를 찾을 수 없습니다!")
            print("[ TrafficSpeed.py ][ ERROR ] TrafficSpdMgr 객체를 생성할 때, 폴더 위치를 확인해주세요!")
            exit(-1)

        self.FILTERED_CSV_DIR = FILTERED_CSV_DIR
        self.pandasDF = None
        self.fileName = None

    def setPandasDF(self, file_name: str):
        ''' setPandasDF는 file_name 을 이용하여
        csv를 Pandas Dataframe으로 불러옵니다.

        :param file_name: 날짜에 따른 CSV 파일을 불러옵니다.
        :return: None
        '''

        if self.fileName != file_name:
            self.fileName = file_name
            self.pandasDF = pd.read_csv(file_name)

        return self.pandasDF

    def getSpeed(self, year: int, month: int, day: int, hour: int, minute: int,
                 link_id: int, errMin=5) -> int or np.nan:
        ''' 해당 시각과 Link ID에 해당되는 속도값을 반환합니다.
        해당 시각에 시간이 존재하지 않으면 errMin 오차 내의 해당되는 시각의 속도값을 반환합니다.
        가량 errMin=5 이고, 얻고자 하는 속도값의 시간이 0시 57분 일 경우 속도값이 존재하지 않습니다.
        하여 0시 52분 ~ 1시 12분 사이에서 0시 57분과 가장 근사한 시간대의 속도값을 반환합니다.

        [ 경고 ]
        해당 함수에 날짜 A 를 넣고 호출할 경우, 날짜 A 에 대한 Pandas Dataframe이 객체에 저장됩니다.
        하여 같은 날짜 A 에 대한 속도값 을 조회할 경우 속도가 빠르나, 다른 날짜 B 에 대한 속도값을 조회할 경우
        날짜 B 에 대한 Pandas Dataframe이 객체에 다시 저장되기에 이 순간에는 속도가 느립니다.

        하여 많은 데이터를 검색할 때 날짜를 오름차순이나 내림차순으로 정리하여 검색하는 것이 속도가 빠릅니다.

        :param year: 검색하고자 하는 연도
        :param month: 검색하고자 하는 월
        :param day: 검색하고자 하는 날짜
        :param hour: 검색하고자 하는 시간
        :param minute: 검색하고자 하는 분
        :param link_id: 검색하고자 하는 도로 LINK ID
        :param errMin: 검색 오차 범위 ( 단위 : 분 )
        :return: int 5분 단위의 평균 속도 or np.nan ( = 오차 범위 내에 값이 존재하지 않는 경우 )
        '''

        date = datetime.datetime(year=year, month=month, day=day, hour=hour,
                                 minute=minute)
        str_date = date.strftime("%Y%m%d")
        search_date = date.strftime("%Y-%m-%d %H:%M:00.000")
        file_name = self.FILTERED_CSV_DIR + str_date + "_5Min.csv"

        if os.path.exists(file_name) is False:
            return np.nan

        df = self.setPandasDF(file_name)
        df_copy = df.loc[df[columns[1]] == int(link_id)]

        if len(df_copy) == 0:
            return np.nan

        timestamp = pd.to_datetime(df_copy[columns[0]])
        dt = pd.to_datetime(search_date)
        find_index = (abs(dt - timestamp)).idxmin()

        result = df_copy.loc[find_index]
        find_date = timestamp.loc[find_index]

        if find_date > date:
            time_delta = find_date - date
        else:
            time_delta = date - find_date

        errMin_delta = datetime.timedelta(minutes=errMin)

        if errMin_delta > time_delta:
            return result[2]
        else:
            return np.nan


if __name__ == "__main__":
    tr = TrafficSpdMgr("trafficSPDCSV/")
    # spd = tr.getSpeed(2020, 1, 1, 0, 44, 1000000301)
    # print(spd)
    traffic = pd.read_csv("Accident_dataset.csv", encoding="cp949")
    traffic_2 = pd.read_csv("modified_accident_2.csv", encoding="cp949")
    date = list(traffic["datetime"])
    time = list(traffic_2["time"])
    ID = pd.read_csv("Not_Accident_dataset_link_list.csv", encoding="cp949")
    link_ID = list(ID["link"])
    speed = []
    for i in range(len(date)):
        date[i] = date[i].split(" ")
        date[i][0] = date[i][0].split("-")
        time[i] = time[i].split(":")
        spd = tr.getSpeed(int(date[i][0][0]), int(date[i][0][1]), int(date[i][0][2]), int(time[i][0]), int(time[i][1]), int(link_ID[i]))
        speed.append(spd)
        if i % 100 == 0:
            print(i," 진행완료")
    with open('speed.txt', 'w') as f:
        for item in speed:
            f.write("%s\n" % item)