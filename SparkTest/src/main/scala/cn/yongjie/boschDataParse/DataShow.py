from pyspark.sql import SparkSession
import pickle
import os
import matplotlib.pyplot as plt
import numpy as np
import sys
import pandas as pd


class Config:
    
    BASEINPUTPATH = "D:/ImpTask/01.PySparkVichileBigDataParse/04.ScalaProgram/BAICDataParse/output/"
    BASEOUTPUTPATH = "D:/ImpTask/01.PySparkVichileBigDataParse/04.ScalaProgram/BAICDataParse/out/"
    DAY = "year2019/month8/day16/"

    Temperature = "temperature/"
    FuelConsume = "fuelConsume/"
    Speed = "speed/"
    Accelerate = "accelerate/"
    SpeedAcc = "speedAcc/"
    DriveDisPerDay = "driveDisPerDay/"
    DriveDisPerTrip = "driveDisPerTrip/"
    DriveTimePerTrip = "driveTimePerTrip/"
    EngineSpeed = "engineSpeed/"
    GearBox = "gearBox/"

    temperatureInputPath = BASEINPUTPATH + DAY + Temperature
    fuelConsumeInputPath = BASEINPUTPATH + DAY + FuelConsume
    speedInputPath = BASEINPUTPATH + DAY + Speed
    accelerateInputPath = BASEINPUTPATH + DAY + Accelerate
    driveDisPerDayInputPath = BASEINPUTPATH + DAY + DriveDisPerDay
    driveDisPerTripInputPath = BASEINPUTPATH + DAY + DriveDisPerTrip
    driveTimePerTripInputPath = BASEINPUTPATH + DAY + DriveTimePerTrip
    engineSpeedInputPath = BASEINPUTPATH + DAY + EngineSpeed
    gearBoxInputPath = BASEINPUTPATH + DAY + GearBox
    speedAccInputPath = BASEINPUTPATH + DAY + SpeedAcc

    temperatureOutPath = BASEOUTPUTPATH + DAY + Temperature
    fuelConsumeOutPath = BASEOUTPUTPATH + DAY + FuelConsume
    speedOutPath = BASEOUTPUTPATH + DAY + Speed
    accelerateOutPath = BASEOUTPUTPATH + DAY + Accelerate
    driveDisPerDayOutPath = BASEOUTPUTPATH + DAY + DriveDisPerDay
    driveDisPerTripOutPath = BASEOUTPUTPATH + DAY + DriveDisPerTrip
    driveTimePerTripOutPath = BASEOUTPUTPATH + DAY + DriveTimePerTrip
    engineSpeedOutPath = BASEOUTPUTPATH + DAY + EngineSpeed
    gearBoxOutPath = BASEOUTPUTPATH + DAY + GearBox
    speedAccOutPath = BASEOUTPUTPATH + DAY + SpeedAcc


class DataShow:

    @staticmethod
    def plot_histogram(rdd,
                       bucket_min,
                       bucket_max,
                       bucket_width,
                       static_variable,
                       base_output_path):

        bucket_bins = list(np.arange(bucket_min, bucket_max, bucket_width))
        # counts[i] = sum{ edges[i-1] <= values[j] < edges[i] : j }
        hist = rdd.histogram(bucket_bins)[1]

        if not os.path.exists(base_output_path):
            os.makedirs(base_output_path)

        fig_out_path = base_output_path + static_variable + "_" + str(bucket_width) + ".png"
        hist_out_path = base_output_path + static_variable + "_" + str(bucket_width) + ".pk"

        # out figure
        plt.figure(figsize=(20, 10))
        plt.set_cmap('RdBu')
        x = bucket_bins[: -1]
        plt.bar(x, hist, width=bucket_width, align="center", color="r", edgecolor="k", linewidth=0.2)
        plt.title("The static histogram of " + static_variable + " Period = " + str(bucket_width))
        plt.xlabel(static_variable + 'period')
        plt.ylabel("Frequency")
        plt.savefig(fig_out_path)
        plt.close()

        # out result of hist
        zip_hist = list(zip(bucket_bins[: -1], hist))
        with open(hist_out_path, "wb") as f:
            pickle.dump(zip_hist, f)

    @staticmethod
    def plot_histogram_with_bin(rdd, bucket_bins, x_labels, static_variable, base_output_path):

        hist = rdd.histogram(bucket_bins)[1]

        fig_out_path = base_output_path + static_variable + ".png"
        hist_out_path = base_output_path + static_variable + ".pk"

        if not os.path.exists(base_output_path):
            os.makedirs(base_output_path)

        # out figure
        plt.figure(figsize=(20, 10))
        plt.set_cmap('RdBu')
        x = list(range(len(bucket_bins) - 1))
        plt.bar(x, hist, width=1, align="center", color="r", edgecolor="k", linewidth=0.2)
        plt.xticks(x, x_labels)
        plt.title("The static histogram of " + static_variable)
        plt.xlabel(static_variable + 'period')
        plt.ylabel("Frequency")
        plt.savefig(fig_out_path)
        plt.close()

        # out result of hist
        zip_hist = list(zip(bucket_bins[: -1], hist))
        with open(hist_out_path, "wb") as f:
            pickle.dump(zip_hist, f)

    @staticmethod
    def showTemperatureFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowTemperatureFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.temperatureInputPath)

        intake_temp_list = [10]
        rdd_temp = df.rdd.map(lambda x: x["intakeTemperatureSize"])

        for intake_temp in intake_temp_list:
            DataShow.plot_histogram(rdd_temp, 0, 80, intake_temp, "intake_tempurature", Config.temperatureOutPath)

    @staticmethod
    def showFuelConsumptionFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowFuelConsumptionFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.fuelConsumeInputPath)

        rdd_fuel = df.rdd.map(lambda x: x["meterInstantaneousFuelConsumption"])

        fuel_csp_list = [1, 1.5, 2, 2.5, 3]
        for fuel_csp in fuel_csp_list:
            DataShow.plot_histogram(rdd_fuel, 0, 21, fuel_csp, "fuel_consumpation", Config.fuelConsumeOutPath)

    @staticmethod
    def showSpeedFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowSpeedFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.speedInputPath)

        rdd_speed = df.rdd.map(lambda x: x["carSpeed"])
        # speed_list = [-10, -1, 0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200]
        # speed_labels = [str(_) for _ in speed_list[:-1]]
        # DataShow.plot_histogram_with_bin(rdd_speed, speed_list, speed_labels, "speed", Config.speedOutPath)

        speed_period_list = [1.25, 2.5, 5]
        for speed_period in speed_period_list:
            DataShow.plot_histogram(rdd_speed, 0, 200, speed_period, "speed", Config.speedOutPath)

    @staticmethod
    def showAccFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowAccFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.accelerateInputPath)

        rdd_acc = df.rdd.map(lambda x: x["acc"])
        acc_period_list = [0.2, 0.3, 0.4, 0.5]
        for acc_period in acc_period_list:
            DataShow.plot_histogram(rdd_acc, -8, 5, acc_period, "accelerate", Config.accelerateOutPath)

    @staticmethod
    def showDisPerDayFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowDisPerDayFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.driveDisPerDayInputPath)

        rdd_distance_per_day = df.rdd.map(lambda x: x["distance"])
        driving_distance_list = [0, 0.5, 1, 2, 5, 10, 20, 50, 100, 4000]
        distance_labels = ["0-0.5km", "0.5-1km", "1-2km", "2-5km", "5-10km", "10-20km", "20-50km", "50-100km", ">100km"]

        DataShow.plot_histogram_with_bin(rdd_distance_per_day, driving_distance_list, distance_labels,
                                         "driving_distance_per_day", Config.driveDisPerDayOutPath)

    @staticmethod
    def showDisPerTripFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowDisPerTripFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.driveDisPerTripInputPath)

        rdd_cycle_distance = df.rdd.map(lambda x: x["distance_delta"])
        driving_distance_list = [0, 0.5, 1, 2, 5, 10, 20, 50, 100, 4000]
        distance_labels = ["0-0.5km", "0.5-1km", "1-2km", "2-5km", "5-10km", "10-20km", "20-50km", "50-100km", ">100km"]
        DataShow.plot_histogram_with_bin(rdd_cycle_distance, driving_distance_list, distance_labels,
                                         "driving_distance_per_trip", Config.driveDisPerTripOutPath)

    @staticmethod
    def showTimePerTripFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowTimePerTripFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.driveTimePerTripInputPath)

        rdd_cycle_time = df.rdd.map(lambda x: x["ts_delta"])
        cycle_time_list = [0, 1, 5, 10, 30, 60, 120, 300, 3000]
        x_labels = ["0-1min", "1-5min", "5-10min", "10-30min", "30-60min", "60-120min", "120-300min", ">300min"]
        DataShow.plot_histogram_with_bin(rdd_cycle_time, cycle_time_list, x_labels,
                                         "driving_time_per_trip", Config.driveTimePerTripOutPath)

    @staticmethod
    def showTimePerTripFigure():

        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowTimePerTripFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.driveTimePerTripInputPath)

        rdd_cycle_time = df.rdd.map(lambda x: x["ts_delta"])
        cycle_time_list = [0, 1, 5, 10, 30, 60, 120, 300, 3000]
        x_labels = ["0-1min", "1-5min", "5-10min", "10-30min", "30-60min", "60-120min", "120-300min", ">300min"]
        DataShow.plot_histogram_with_bin(rdd_cycle_time, cycle_time_list, x_labels,
                                         "driving_time_per_trip", Config.driveTimePerTripOutPath)

    @staticmethod
    def showEngineSpeedFigure():
        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowEngineSpeedFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.engineSpeedInputPath)

        rdd_engine_speed = df.rdd.map(lambda x: x["engineSpeed"])
        engine_speed_period_list = [100, 200, 300, 400]
        for engine_speed in engine_speed_period_list:
            DataShow.plot_histogram(rdd_engine_speed, 0, 4000, engine_speed, "engineSpeed", Config.engineSpeedOutPath)

    @staticmethod
    def showGearBoxFigure():
        spark = SparkSession. \
            builder. \
            master("local[3]"). \
            appName("ShowGearBoxFigure"). \
            getOrCreate()

        df = spark. \
            read. \
            option("mergeSchema", "true"). \
            parquet(Config.gearBoxInputPath)

        rdd_gear_box = df.rdd.map(lambda x: x["gearBox"])
        gear_box_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        x_labels = [str(x) for x in gear_box_list[:-1]]
        DataShow.plot_histogram_with_bin(rdd_gear_box, gear_box_list, x_labels,
                                         "gearBox", Config.gearBoxOutPath)

    @staticmethod
    def showSpeedAndAccFigure():

        inputFile = ""
        for file in os.listdir(Config.speedAccInputPath):
            if file.endswith(".csv"):
                inputFile = os.path.join(Config.speedAccInputPath, file)

        if not os.path.exists(Config.speedAccOutPath):
            os.makedirs(Config.speedAccOutPath)

        dfSpeedAcc = pd.read_csv(inputFile)
        vin_list = dfSpeedAcc["vin"].unique()
        for vin in vin_list[0: 10]:
            plt.figure()
            dfSpeedAcc[dfSpeedAcc["vin"] == vin].plot.scatter(x="carSpeed", y="acc")
            plt.title(vin)
            figureOutPath = Config.speedAccOutPath + vin + ".png"
            plt.savefig(figureOutPath)
            plt.close()


if __name__ == "__main__":

    variable = sys.argv[1]

    if variable == "temperature":
        DataShow.showTemperatureFigure()
    if variable == "fuelConsume":
        DataShow.showFuelConsumptionFigure()
    if variable == "speed":
        DataShow.showSpeedFigure()
    if variable == "accelerate":
        DataShow.showAccFigure()
    if variable == "driveDisPerDay":
        DataShow.showDisPerDayFigure()
    if variable == "drivePerTrip":
        DataShow.showDisPerTripFigure()
        DataShow.showTimePerTripFigure()
    if variable == "engineSpeed":
        DataShow.showEngineSpeedFigure()
    if variable == "gearBox":
        DataShow.showGearBoxFigure()
    if variable == "speedAcc":
        DataShow.showSpeedAndAccFigure()

    if variable == "all":
        DataShow.showTemperatureFigure()
        DataShow.showFuelConsumptionFigure()
        DataShow.showSpeedFigure()
        DataShow.showAccFigure()
        DataShow.showDisPerDayFigure()
        DataShow.showDisPerTripFigure()
        DataShow.showTimePerTripFigure()
        DataShow.showEngineSpeedFigure()
        DataShow.showGearBoxFigure()
        DataShow.showSpeedAndAccFigure()




