#!/usr/bin/env python3

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any

import matplotlib.pyplot as plt
import matplotlib.ticker as tk
import pandas as pd
import pandas_datareader.data as web

# For pandas_datareader, sometimes there can be version mismatch
pd.core.common.is_list_like = pd.api.types.is_list_like

# Pretty printing of pandas dataframe
pd.set_option("expand_frame_repr", False)

# Main menu options
UPDATE_OPTION = 1
VISUALIZE_OPTION = 2
EXIT_OPTION = 3

# Visualize menu options
SUMMARY_OPTION = 1
GRAPHIC_OPTION = 2
BACK_OPTION = 3


class Stock:
    def __init__(self: Any) -> None:
        self.connect()

    def connect(self: Any) -> None:
        stock_exists = os.path.exists("stocks.db")
        self.__engine = sqlite3.connect("stocks.db")
        self.__cursor = self.__engine.cursor()

        if not stock_exists:
            stocks = pd.DataFrame({
                "Stock": [],
                "Date": [],
                "High": [],
                "Low": [],
                "Open": [],
                "Close": [],
                "Volume": [],
                "Adj_Close": []
            })
            ranges = pd.DataFrame({
                "Stock": [],
                "Start": [],
                "End": []
            })
            stocks.to_sql("stocks", con=self.__engine, index=False)
            ranges.to_sql("ranges", con=self.__engine, index=False)

    def ranges_from_sql(self: Any, stock_name: str = "") -> pd.DataFrame:
        if stock_name:
            return pd.read_sql_query(
                f"SELECT * FROM ranges WHERE Stock='{stock_name}' ORDER BY Start, End", con=self.__engine)

        else:
            return pd.read_sql_query(
                f"SELECT * FROM ranges ORDER BY Stock, Start, End", con=self.__engine)

    def ranges_to_sql(self: Any, stock_name: str, ranges: pd.DataFrame) -> None:
        # Delete existing stock ranges
        self.__cursor.execute(f"DELETE FROM ranges WHERE Stock='{stock_name}'")
        self.__engine.commit()

        # Insert modified ranges
        ranges.to_sql("ranges", con=self.__engine,
                      if_exists="append", index=False)

    def stock_from_web(self: Any, stock_name: str, start: datetime, end: datetime) -> pd.DataFrame:
        # Handle connection errors
        while True:
            try:
                df = web.get_data_yahoo(stock_name, start=start, end=end)
                break

            except Exception:
                retry = input(
                    "Please verify your connection. Retry? (y/n) ").lower()

                if retry == "n":
                    print()

                    return pd.DataFrame({
                        "Stock": [],
                        "Date": [],
                        "High": [],
                        "Low": [],
                        "Open": [],
                        "Close": [],
                        "Volume": [],
                        "Adj_Close": []
                    })

        # Use numerical integer index instead of date
        df = df.reset_index()

        # Insert first column with the stock name
        df.insert(0, "Stock", stock_name)

        # Databases do not like column names that contain spaces
        df.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)

        # Convert Date column to string
        df.Date = df.Date.dt.date.astype("string")

        return df

    def stock_from_sql(self: Any, stock_name: str) -> pd.DataFrame:
        return pd.read_sql_query(
            f"SELECT * FROM stocks WHERE Stock='{stock_name}' ORDER BY Date", con=self.__engine)

    def stock_to_sql(self: Any, df: pd.DataFrame) -> None:
        df.to_sql("stocks", con=self.__engine, if_exists="append", index=False)

    def close(self: Any) -> None:
        self.__cursor.close()
        self.__engine.close()


def main(stock: Stock) -> None:
    while True:
        print(">>> 1.Actualización de datos")
        print(">>> 2.Visualización de datos")
        print(">>> 3.Salir")

        try:
            option = int(input())

            if option == UPDATE_OPTION:
                update(stock)

            elif option == VISUALIZE_OPTION:
                visualize(stock)

            elif option == EXIT_OPTION:
                break

            else:
                raise ValueError

        except ValueError:
            print(">>> Opción incorrecta. Vuelva a intentarlo.\n")


def update(stock: Stock) -> None:
    print(">>> Ingrese ticker a pedir:")
    stock_name = input().upper()

    try:
        print(">>> Ingrese fecha de inicio:")
        start_str = input()
        start = datetime.strptime(start_str, "%Y/%m/%d")

        print(">>> Ingrese fecha de fin:")
        end_str = input()
        end = datetime.strptime(end_str, "%Y/%m/%d")
        yesterday = datetime.strptime(datetime.now().strftime(
            "%Y/%m/%d"), "%Y/%m/%d") - timedelta(days=1)

        if yesterday < end:
            end = yesterday
            end_str = yesterday.strftime("%Y/%m/%d")

        # Fix ranges
        ranges_df = stock.ranges_from_sql(stock_name)

        for _index, row in ranges_df.iterrows():
            if row.Start <= start_str and start_str <= row.End:
                start = datetime.strptime(
                    row.End, "%Y/%m/%d") + timedelta(days=1)
                start_str = start.strftime("%Y/%m/%d")

            if row.Start <= end_str and end_str <= row.End:
                end = datetime.strptime(
                    row.Start, "%Y/%m/%d") - timedelta(days=1)
                end_str = end.strftime("%Y/%m/%d")

        if start > end:
            raise ValueError

        ranges_df.loc[len(ranges_df.index)] = [stock_name, start_str, end_str]
        ranges_df.sort_values(by=["Start", "End"],
                              inplace=True, ignore_index=True)

        new_ranges_df = pd.DataFrame({
            "Stock": [],
            "Start": [],
            "End": []
        })

        if (len(ranges_df.index)):
            start_str, end_str = ranges_df.loc[0, ["Start", "End"]]

            for _index, row in ranges_df.iterrows():
                before_start_str = (datetime.strptime(
                    row.Start, "%Y/%m/%d") - timedelta(days=1)).strftime("%Y/%m/%d")

                if before_start_str <= end_str:
                    if end_str < row.End:
                        end_str = row.End

                else:
                    new_ranges_df.loc[len(new_ranges_df.index)] = [
                        stock_name, start_str, end_str]
                    start_str = row.Start
                    end_str = row.End

            new_ranges_df.loc[len(new_ranges_df.index)] = [
                stock_name, start_str, end_str]

    except ValueError:
        print("El rango de fechas es incorrecto o ya se encuentra en la base de datos. Vuelva a intentarlo.\n")

        return

    print(">>> Pidiendo datos ...")

    # Request stock from the web
    web_df = stock.stock_from_web(stock_name, start, end)

    if len(web_df.index) == 0:
        return

    # Avoid duplicate records
    sql_df = stock.stock_from_sql(stock_name)
    web_df = web_df[web_df.Date.isin(sql_df.Date) == False]

    # Store stock to the database
    stock.stock_to_sql(web_df)

    # Store ranges to the database
    stock.ranges_to_sql(stock_name, new_ranges_df)

    print(">>> Datos guardados correctamente\n")


def visualize(stock: Stock) -> None:
    while True:
        print(">>> 1.Resumen")
        print(">>> 2.Gráfico de ticker")
        print(">>> 3.Regresar al menú principal")

        try:
            option = int(input())

            if option == SUMMARY_OPTION:
                summary(stock)

            elif option == GRAPHIC_OPTION:
                graphic(stock)

            elif option == BACK_OPTION:
                break

            else:
                raise ValueError

        except ValueError:
            print("Opción incorrecta. Vuelva a intentarlo.\n")


def summary(stock: Stock) -> None:
    print(">>> Los tickers guardados en la base de datos son:")
    ranges_df = stock.ranges_from_sql()

    # Retrieve the longest stock name to format output
    longest = ranges_df.Stock.str.len().max()

    for _index, row in ranges_df.iterrows():
        print(f">>> {row.Stock.ljust(longest)} - {row.Start} <-> {row.End}")

    print()


def graphic(stock: Stock) -> None:
    # This function can be improved by implementing Start and End dates filtering
    print(">>> Ingrese el ticker a graficar:")
    stock_name = input().upper()

    # Retrieve stock from database
    df = stock.stock_from_sql(stock_name)

    # Build Open value per Date graphic
    time_series = df.Open.tolist()
    dt_list = df.Date.tolist()
    fig, ax = plt.subplots()
    ax.xaxis.set_major_locator(tk.MaxNLocator("auto"))
    ax.plot(dt_list, time_series, linewidth=2)
    fig.autofmt_xdate()
    plt.show()


if __name__ == "__main__":
    stock = Stock()
    main(stock)
    stock.close()
