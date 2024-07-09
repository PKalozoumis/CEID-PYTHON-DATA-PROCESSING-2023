import urllib.request
import csv
import sys
import os.path

import math

import pandas as pd
import numpy as np

from matplotlib import pyplot as plt
from matplotlib import patches
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

import tkinter as tk
from tkinter import ttk
from tkinter import filedialog

import mysql.connector

#This list contains the DataFrames that will be exported to csv files
#These will also be inserted to the database, after resetting the index first
data_list = []

#=======================================================================================================================================================

def connect_to_database():
    '''
    Connects to the database and returns the connection object
    '''
    try:
        con = mysql.connector.connect(
        host='localhost',
        user='python_user',
        database='python_project_2023'
        )
    except Exception as e:
        raise e

    return con

#=======================================================================================================================================================

def insert_into_database(con, cur):
    '''
    Destroys and recreates all the necessary tables and inserts data from data_list into them
    '''
    global data_list

    cur.execute(
    '''
    DROP TABLE IF EXISTS
    value_per_month,
    value_per_country,
    value_per_transport,
    value_per_weekday,
    value_per_commodity,
    five_months_highest_value,
    max_comms_per_country,
    biggest_value_date
    ''')

    #value_per_month
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE value_per_month
    (
        vpm_measure ENUM('$', 'Tonnes') NOT NULL,
        vpm_year ENUM('2015', '2016', '2017', '2018', '2019', '2020', '2021') NOT NULL,
        vpm_month ENUM('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12') NOT NULL,
        vpm_value BIGINT NOT NULL,

        PRIMARY KEY(vpm_measure, vpm_year, vpm_month)
    )
    '''
    )
    to_db = data_list[0].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Year": "str",
        "Month": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO value_per_month VALUES(%s, %s, %s, %s)", (row["Measure"], row["Year"], row["Month"], row["Value"]))

    #value_per_country
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE value_per_country
    (
        vpc_measure ENUM('$', 'Tonnes') NOT NULL,
        vpc_country ENUM('All','Australia','China','East Asia (excluding China)','European Union (27)','Japan','Total (excluding China)','United Kingdom','United States') NOT NULL,
        vpc_value BIGINT NOT NULL,

        PRIMARY KEY(vpc_measure, vpc_country)
    )
    '''
    )

    to_db = data_list[1].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Country": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO value_per_country VALUES(%s, %s, %s)", (row["Measure"], row["Country"], row["Value"]))

    #value_per_transport
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE value_per_transport
    (
        vpt_measure ENUM('$', 'Tonnes') NOT NULL,
        vpt_mode ENUM('Air', 'All', 'Sea') NOT NULL,
        vpt_value BIGINT NOT NULL,

        PRIMARY KEY(vpt_measure, vpt_mode)
    )
    '''
    )

    to_db = data_list[2].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Transport_Mode": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO value_per_transport VALUES(%s, %s, %s)", (row["Measure"], row["Transport_Mode"], row["Value"]))

    #value_per_weekday
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE value_per_weekday
    (
        vpw_measure ENUM('$', 'Tonnes') NOT NULL,
        vpw_weekday ENUM('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday') NOT NULL,
        vpw_value BIGINT NOT NULL,

        PRIMARY KEY(vpw_measure, vpw_weekday)
    )
    '''
    )

    to_db = data_list[3].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Weekday": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO value_per_weekday VALUES(%s, %s, %s)", (row["Measure"], row["Weekday"], row["Value"]))

    #value_per_commodity
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE value_per_commodity
    (
        vpc_measure ENUM('$', 'Tonnes') NOT NULL,
        vpc_commodity ENUM('All', 'Electrical machinery and equip', 'Fish, crustaceans, and molluscs', 'Fruit', 'Logs, wood, and wood articles', 'Meat and edible offal', 'Mechanical machinery and equip', 'Milk powder, butter, and cheese', 'Non-food manufactured goods') NOT NULL,
        vpc_value BIGINT NOT NULL,

        PRIMARY KEY(vpc_measure, vpc_commodity)
    )
    '''
    )

    to_db = data_list[4].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Commodity": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO value_per_commodity VALUES(%s, %s, %s)", (row["Measure"], row["Commodity"], row["Value"]))

    #five_months_highest_value
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE five_months_highest_value
    (
        fmv_measure ENUM('$', 'Tonnes') NOT NULL,
        fmv_year ENUM('2015', '2016', '2017', '2018', '2019', '2020', '2021') NOT NULL,
        fmv_month ENUM('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12') NOT NULL,
        fmv_value BIGINT NOT NULL,

        PRIMARY KEY(fmv_measure, fmv_year, fmv_month)
    )
    '''
    )

    to_db = data_list[5].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Year": "str",
        "Month": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO five_months_highest_value VALUES(%s, %s, %s, %s)", (row["Measure"], row["Year"], row["Month"], row["Value"]))

    #max_comms_per_country
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE max_comms_per_country
    (
        mpc_measure ENUM('$', 'Tonnes') NOT NULL,
        mpc_country ENUM('All','Australia','China','East Asia (excluding China)','European Union (27)','Japan','Total (excluding China)','United Kingdom','United States') NOT NULL,
        mpc_commodity ENUM('All', 'Electrical machinery and equip', 'Fish, crustaceans, and molluscs', 'Fruit', 'Logs, wood, and wood articles', 'Meat and edible offal', 'Mechanical machinery and equip', 'Milk powder, butter, and cheese', 'Non-food manufactured goods') NOT NULL,
        mpc_value BIGINT NOT NULL,

        PRIMARY KEY(mpc_measure, mpc_country, mpc_commodity)
    )
    '''
    )

    to_db = data_list[6].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Country": "str",
        "Commodity": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO max_comms_per_country VALUES(%s, %s, %s, %s)", (row["Measure"], row["Country"], row["Commodity"], row["Value"]))

    #biggest_value_date
    #============================================================================
    cur.execute(
    '''
    CREATE TABLE biggest_value_date
    (
        bvd_measure ENUM('$', 'Tonnes') NOT NULL,
        bvd_commodity ENUM('All', 'Electrical machinery and equip', 'Fish, crustaceans, and molluscs', 'Fruit', 'Logs, wood, and wood articles', 'Meat and edible offal', 'Mechanical machinery and equip', 'Milk powder, butter, and cheese', 'Non-food manufactured goods') NOT NULL,
        bvd_date DATETIME NOT NULL,
        bvd_value BIGINT NOT NULL,

        PRIMARY KEY(bvd_measure, bvd_commodity)
    )
    '''
    )

    to_db = data_list[7].reset_index()
    to_db = to_db.astype({
        "Measure": "str",
        "Commodity": "str",
        "Date": "str",
        "Value": "int"
    })

    for _, row in to_db.iterrows():
        cur.execute("INSERT INTO biggest_value_date VALUES(%s, %s, %s, %s)", (row["Measure"], row["Commodity"], row["Date"], row["Value"]))

    con.commit()

#=======================================================================================================================================================

def appfunc():
    '''
    The graphical application
    '''
    global data_list
    cur = None
    con = None

    window = tk.Tk()
    window.title("Application")
    window.geometry("960x720")

    plot_canvas = None

    #Functions
    #=======================================================================================================================

    #Notes about plots:
        #8 categories of plots total, from 0 to 7 (all the requested plots)
        #Each has two units
        #For category i:
        #   i*2 + 1 -> $
        #   i*2 + 2 -> Tonnes

    def save_figure(plot: int, unit: str):

        if unit == "$":
            plot = 2*plot + 1
        elif unit == "Tonnes":
            plot = 2*plot + 2

        path = filedialog.asksaveasfilename(defaultextension = ".png", title = "Save Plot")

        if path:
            plt.figure(plot).savefig(path)

    def export_to_csv(plot: int):

        path = filedialog.asksaveasfilename(defaultextension = ".csv", title = "Export as CSV")

        if path:
            data_list[plot].to_csv(path)

    def draw_figure(plot: int, unit: str):
        nonlocal plot_canvas

        if unit == "$":
            plot = 2*plot + 1
        elif unit == "Tonnes":
            plot = 2*plot + 2

        if plot_canvas != None:
            plot_canvas.get_tk_widget().delete('all')

        plot_canvas = FigureCanvasTkAgg(plt.figure(plot), master=window)
        plot_canvas.draw()
        plot_canvas.get_tk_widget().place(x = 0, y=101)

    #=======================================================================================================================

    #Default figure that appears on the screen
    draw_figure(0,"$")

    #Top of Screen
    #=======================================================================================================================
    canvas = tk.Canvas(window, width=960, height=100)
    canvas.place(x = 0, y = 0)
    canvas.create_rectangle(0, 0, 960, 100, fill="#ccffff")
    #=======================================================================================================================
    
    #Variables
    unit = tk.StringVar(window, "$") #We need to know which unit we've selected, so that we can show the respective plot
    kwargs = {"bg": "#ccffff", "activebackground": "#39698a"}

    #Figure Combo Box
    #=======================================================================================================================
    figure_combo_box = ttk.Combobox(window, state="readonly")
    figure_combo_box["values"] = (
        "Value per Month",
        "Value per Country",
        "Value per Transport Mode",
        "Value per Weekday",
        "Value per Commodity",
        "5 Months with Biggest Value",
        "5 Commodities with Biggest Value per Country",
        "Biggest Value Day per Commodity"
    )
    figure_combo_box.current(0)
    figure_combo_box.bind("<<ComboboxSelected>>", lambda _: draw_figure(figure_combo_box.current(), unit.get()))
    figure_combo_box.place(x = 10, y = 40)
    #=======================================================================================================================

    #Labels
    #=======================================================================================================================

    label_1 = tk.Label(window, text = "Select a figure:", **kwargs)
    label_1.place(x = 10, y=10)

    label_2 = tk.Label(window, text = "Unit:", **kwargs)
    label_2.place(x = 250, y=10)

    #=======================================================================================================================


    #Radio buttons
    #=======================================================================================================================
    rb_unit_1 = tk.Radiobutton(window, text="$", variable = unit, command = lambda: draw_figure(figure_combo_box.current(), "$"), value = "$", **kwargs)
    rb_unit_2 = tk.Radiobutton(window, text="Tonnes", variable = unit, command = lambda: draw_figure(figure_combo_box.current(), "Tonnes"), value = "Tonnes", **kwargs)

    rb_unit_1.place(x = 250, y=40)
    rb_unit_2.place(x = 300, y = 40)
    #=======================================================================================================================

    #The buttons
    #=======================================================================================================================
    save_button = tk.Button(window, text="Save Plot", command = lambda: save_figure(figure_combo_box.current(), unit.get()), **kwargs)
    save_button.place(x=800,y=10)

    export_button = tk.Button(window, text="Export Data as CSV", command = lambda: export_to_csv(figure_combo_box.current()), **kwargs)
    export_button.place(x=800,y=50)
    #=======================================================================================================================

    #Close handler
    #=======================================================================================================================
    def close():
        '''
        Handler for when we close the window
        Contains a message box asking the user if they want to quit
        If the user quits, the window closes, all the plots close, and we disconnect from the database
        '''
        modal = tk.Toplevel(window)
        modal.transient(window)
        modal.withdraw()
        modal.grab_set()

        opt = tk.messagebox.askyesno("Quit", "Are you sure you want to quit?")

        modal.grab_release()
        modal.destroy()

        if opt:
            window.destroy()
            plt.close("all")
            if cur is not None:
                cur.close()

            if con is not None:
                con.close()

    window.protocol("WM_DELETE_WINDOW", close)
    #=======================================================================================================================

    #Connect to the database
    #If connection fails, the user can still proceed, but without saving to the database
    #If connection succeeds, all the data gets stored to the database
    #=======================================================================================================================
    cont = False

    try:
        con = connect_to_database()
    except Exception as e:
        window.withdraw()
        cont = tk.messagebox.askyesno("MySQL Error", "Failed to connect to the MySQL database. Would you like to continue without saving to the database?")

        if not cont:
            window.destroy()
            plt.close("all")
            return
        else:
            window.deiconify()
    
    if con is not None:
        cur = con.cursor()
        insert_into_database(con, cur)
    #=======================================================================================================================

    tk.mainloop()

#=======================================================================================================================================================

def day_sorting(col: pd.Series) -> pd.Series:
    '''
    Used as a custom sorting key to sort the days of the week in a Series
    '''
    ret = col

    #Two columns are passed in total, measure and weekday
    #If measure is passed, ignore it, the default order is "$","Tonnes"
    #If weekday is passed, apply custom sorting
        #Days are sorted using their position in the list as the value
        #Essentially, the list defines the order
    if col.name == "Weekday":
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        ret = col.apply(lambda x: days.index(x))

    #Default text sorting will be applied
    return ret

#=======================================================================================================================================================

def sum_of_values(lst: list):
    sum = 0
    for tup in lst:
        sum += tup[1]

    return sum

#=======================================================================================================================================================

def make_dict(ser: pd.Series):
    dictionary = {"Electrical machinery and equip":0, "Logs, wood, and wood articles":0, "Mechanical machinery and equip":0}

    for x in ser:
        dictionary[x[0]] = x[1]

    return dictionary
        

#=======================================================================================================================================================

def download_file() -> list:
    '''
    Downloads the file and returns a list of dictionaries
    '''
    url = "https://www.stats.govt.nz/assets/Uploads/Effects-of-COVID-19-on-trade/Effects-of-COVID-19-on-trade-At-15-December-2021-provisional/Download-data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
    filename = "file.csv"

    if not os.path.isfile(filename):
        print("Downloading file...\n")
        try:
            urllib.request.urlretrieve(url,filename)
        except urllib.error.URLError as e:
            raise e
        print("File downloaded successfully\n")
    else:
        print("File already exists locally\n")

    csv_list = list()

    with open(filename, "r", newline = '') as csv_file:

        #DictReader will use the first row as the key names
        reader = csv.DictReader(csv_file, delimiter = ',')
        csv_list = [row for row in reader]

    return csv_list

#MAIN
#=======================================================================================================================================================

if __name__ == "__main__":
    try:
        csv_list = download_file()
    except urllib.error.URLError as e:
        print("Failed to download the file: ", e)
        sys.exit()

    frame = pd.DataFrame(csv_list)

    #Convert to the types you want
    frame["Date"] = pd.to_datetime(frame["Date"], format="%d/%m/%Y")
    frame["Value"] = pd.to_numeric(frame["Value"])
    frame["Cumulative"] = pd.to_numeric(frame["Cumulative"])

    show_plots = False

    #==================================================================================

    #Value per month

    grouped = frame[["Measure", "Date", "Value"]].groupby([frame["Measure"], frame["Date"].dt.year, frame["Date"].dt.month])
    value_per_month = grouped["Value"].sum()
    value_per_month.index = value_per_month.index.set_names(["Measure", "Year", "Month"])

    #Store the data to be inserted into the database
    export = value_per_month
    data_list.append(export)

    for unit in ["$", "Tonnes"]:

        categories = value_per_month[unit].reset_index().apply(
            lambda x: f"{int(x['Month']):02}-{x['Year']}" if int(x['Month']) == 1 else f"{int(x['Month']):02}", axis=1
        )

        fig, ax = plt.subplots(figsize=(9.6,6.2))

        # Set the new bounds
        left, bottom, width, height = ax.get_position().bounds
        new_bottom = (max([len(s) for s in categories]) + 1)*0.02
        ax.set_position([0.05, new_bottom, width + 0.14, height])

        tic = []

        for i, val in enumerate(value_per_month[unit]):
            tic.append(i)
            ax.bar(i, val, width = 0.8, color=["#3498DB", "#5DADE2"][i%2])

        ax.set_xticks(tic)
        ax.set_xticklabels(labels = categories, rotation='vertical', ha="center")
        ax.set_xlabel("Months")
        ax.set_ylabel("Value")
        ax.set_title("Value per month in " + unit)

        for tick in ax.get_xticklabels():
            tick.set_fontsize(7)

    #==================================================================================

    #Value per country
    grouped = frame[["Measure", "Country", "Value"]].groupby([frame["Measure"], frame["Country"]])
    value_per_country = grouped["Value"].sum()

    #Sort the values for a more ✨ pleasant ✨ presentation
    value_per_country = value_per_country.groupby(level=0, group_keys=False).apply(lambda x: x.sort_values())

    #Store the data to be inserted into the database
    export = value_per_country
    data_list.append(export)

    for unit in ["$", "Tonnes"]:
        categories = value_per_country[unit].reset_index()["Country"]
        fig, ax = plt.subplots(figsize=(9.6,6.2))

        # Set the new bounds
        left, bottom, width, height = ax.get_position().bounds
        new_left = (max([len(s) for s in categories]) + 1)*0.009 #0.013
        new_width = width - (new_left - left)
        ax.set_position([new_left, bottom, new_width, height])

        ax.barh(categories, value_per_country[unit], color=["#3498DB", "#5DADE2"])
        ax.set_xlabel("Value")
        ax.set_ylabel("Country")
        ax.set_title("Value per country in " + unit)

    #==================================================================================

    #Value per transport mode
    grouped = frame[["Measure", "Transport_Mode", "Value"]].groupby([frame["Measure"], frame["Transport_Mode"]])
    value_per_transport = grouped["Value"].sum()

    #Sort the values for a more ✨ pleasant ✨ presentation
    value_per_transport = value_per_transport.groupby(level=0, group_keys=False).apply(lambda x: x.sort_values())

    #Store the data to be inserted into the database
    export = value_per_transport
    data_list.append(export)

    for unit in ["$", "Tonnes"]:
        categories = value_per_transport[unit].reset_index()["Transport_Mode"]

        fig, ax = plt.subplots(figsize=(9.6,6.2))
        
        ax.barh(categories, value_per_transport[unit], height = 0.4, color=["#3498DB", "#5DADE2"])
        ax.set_xlabel("Value")
        ax.set_ylabel("Transport Mode")
        ax.set_title("Value per transport mode in " + unit)

    #==================================================================================

    #Value per weekday
    grouped = frame[["Measure", "Weekday", "Value"]].groupby([frame["Measure"], frame["Weekday"]])
    value_per_weekday = grouped["Value"].sum()

    #Sort the days of the week
    value_per_weekday = value_per_weekday.reset_index().sort_values(["Measure", "Weekday"], key=day_sorting, ascending=False).set_index(["Measure","Weekday"])["Value"]

    #Store the data to be inserted into the database
    export = value_per_weekday
    data_list.append(export)

    for unit in ["$", "Tonnes"]:
        categories = value_per_weekday[unit].reset_index()["Weekday"]

        fig, ax = plt.subplots(figsize=(9.6,6.2))

        ax.barh(categories, value_per_weekday[unit], color=["#3498DB", "#5DADE2"])
        ax.set_xlabel("Value")
        ax.set_ylabel("Weekday")
        ax.set_title("Value per weekday in " + unit)

    #==================================================================================

    #Value per commodity
    grouped = frame[["Measure", "Commodity", "Value"]].groupby([frame["Measure"], frame["Commodity"]])
    value_per_commodity = grouped["Value"].sum()

    #Sort the values for a more ✨ pleasant ✨ presentation
    value_per_commodity = value_per_commodity.groupby(level=0, group_keys=False).apply(lambda x: x.sort_values())

    #Store the data to be inserted into the database
    export = value_per_commodity
    data_list.append(export)

    for unit in ["$", "Tonnes"]:
        categories = value_per_commodity[unit].reset_index()["Commodity"]

        fig, ax = plt.subplots(figsize=(9.6,6.2))

        # Set the new bounds
        left, bottom, width, height = ax.get_position().bounds
        new_left = (max([len(s) for s in categories]) + 1)*0.009 #0.013
        new_width = width - (new_left - left)
        ax.set_position([new_left, bottom, new_width, height])
        
        ax.barh(categories, value_per_commodity[unit], color=["#3498DB", "#5DADE2"])
        ax.set_xlabel("Value")
        ax.set_ylabel("Commodity")
        ax.set_title("Value per commodity in " + unit)

    #==================================================================================

    #5 months with biggest value

    recyclable = ["Electrical machinery and equip", "Logs, wood, and wood articles", "Mechanical machinery and equip"]

    filtered = frame.loc[frame["Commodity"].isin(recyclable)]
    grouped = filtered[["Measure", "Commodity", "Value", "Date"]].groupby([filtered["Measure"], filtered["Date"].dt.year, filtered["Date"].dt.month, filtered["Commodity"]])
    five_month_value = grouped["Value"].sum()
    five_month_value.index = five_month_value.index.set_names(["Measure", "Year", "Month", "Commodity"])

    #Extract commodity from the index, add it as a new column
    #Concatenate the two columns "Commodity" and "Value" into a single column (tuple)
    five_month_value = five_month_value.reset_index(level="Commodity").apply(lambda x: (x["Commodity"], x["Value"]), axis=1)

    #For each month there are multiple pairs of commodity and value
        #Multiple rows with the same month
    #Combine those pairs into a single list
        #One row for each month
    #five_month_value = five_month_value.groupby(["Measure", "Year", "Month"]).apply(list)
    five_month_value = five_month_value.groupby(["Measure", "Year", "Month"]).apply(lambda x: [make_dict(x)])

    #After this point we select only the 5 largest months (for each measure)

    #Create a new column with the sum of all the values for each month, regardless of commodity
    #This is done by applying the function sum_of_values to each element of the column "Value"
    #sum_of_values accepts a list of tuples in the form (commodity, value) and sums up the values
    #In order to add a new column named "Sum", we must also conver the Series into a DataFrame with a single column named "Value"

    five_month_value = five_month_value.to_frame("Value")

    five_month_value["Sum"] = five_month_value["Value"].apply(lambda x: sum(x[0].values()))

    #Extract the 5 months with the largest sum into a list called "largest_months".
    #We will be using this list to select the rows that contain those months
    largest_months = five_month_value["Sum"].groupby("Measure", group_keys=False).apply(lambda x: x.nlargest(5)) #5 largest sums for each measure
    largest_months = largest_months.reset_index()[["Measure", "Year", "Month"]] #Discard sum column and expand the index into 3 columns
    largest_months = list(largest_months.to_records(index=False)) #Create list of max months

    #Store the data to be inserted into the database
    #THIS IS NOT FOR PLOTTING, IT'S ONLY FOR EXPORTING THE DATA
    #We don't need information about each commodity separately, so we only keep the sum
    export = five_month_value.loc[five_month_value.index.isin(largest_months), "Sum"].to_frame("Value")
    data_list.append(export)

    #Select only max months (for each measure separately) and discard the sum column
    five_month_value = five_month_value.loc[five_month_value.index.isin(largest_months), "Value"]

    legend = recyclable
    colors = ["#073763", "#166CAD", "#5DADE2"]

    for unit in ["$", "Tonnes"]:

        fig, ax = plt.subplots(figsize=(9.6,6.2))

        #5 months
        categories = five_month_value[unit].reset_index().apply(lambda x: f"{int(x['Month']):02}-{x['Year']}", axis=1).to_list()

        #List of the five dictionaries (also remember that each dictionary is in a single element list)
        dict_list = five_month_value[unit].to_list()

        stack_data = {key : [x[0][key] for x in dict_list] for key in recyclable}

        stack_size = np.array([0, 0, 0, 0, 0])

        for i in range(0,3):
            ax.barh(categories, stack_data[legend[i]], left = stack_size, color=colors[i])
            stack_size += np.array(stack_data[legend[i]])

        ax.set_title("The 5 months with biggest value in " + unit)
        ax.set_xlabel("Value")
        ax.set_ylabel("Months")
        ax.legend(legend)

    #==================================================================================

    #5 commodities with biggest value for each country

    grouped = frame[["Measure", "Value", "Country", "Commodity"]].groupby(["Measure", "Country", "Commodity"])
    max_commodities_per_country = grouped["Value"].sum()

    max_commodities_per_country = max_commodities_per_country.groupby(["Measure", "Country"], group_keys=False).apply(lambda x: x.nlargest(5))

    #Store the data to be inserted into the database
    export = max_commodities_per_country

    data_list.append(export)

    bar_width = .25

    all_commodities = frame["Commodity"].unique()
    colors = ['#267CB8', '#F6C83F', '#68397A', '#8ABC5A', '#C9371E', '#FF6A00', '#42B5A0', '#32873A', '#FF7F7F']
    color_map = {key:value for key, value in zip(all_commodities, colors)}

    for unit in ["$", "Tonnes"]:
        tick_locations = []
        x = 0
        countries = max_commodities_per_country[unit].reset_index()["Country"].unique()

        fig, ax = plt.subplots(figsize=(9.6,6.2))

        # Set the new bounds
        left, bottom, width, height = ax.get_position().bounds
        new_bottom = (max([len(s) for s in categories]) + 1)*0.025
        ax.set_position([left, new_bottom, width, height - (new_bottom-bottom)])

        for country in countries:
            commodities = max_commodities_per_country[unit][country].reset_index()["Commodity"]

            tick_locations.append(x + 0.25*((len(commodities)-1)//2))

            for com in commodities:
                ax.bar(x, max_commodities_per_country[unit][country][com], bar_width, color=[color_map[com]])
                x += bar_width

            x += bar_width

        ax.set_xticks(tick_locations)
        ax.set_xticklabels(countries, rotation=45, ha="right")

        ax.set_title("The 5 commodities with the biggest value in " + unit + " per country")
        ax.set_xlabel("Countries")
        ax.set_ylabel("Value")

        custom_legend = [patches.Patch(color=COLOR, label=LABEL) for LABEL, COLOR in color_map.items()]
        ax.legend(handles = custom_legend)

    #==================================================================================

    #Biggest value day
    grouped = frame[["Measure", "Value", "Date", "Commodity"]].groupby([frame["Measure"], frame["Commodity"], frame["Date"]])
    biggest_value_day = grouped["Value"].sum()

    #This is now a multi-indexed Series. Apply multi-index grouping
    value2 = biggest_value_day.groupby(["Measure","Commodity"]).apply(lambda x: (x.idxmax(), x[x.idxmax()]))

    #The max dates are in a tuple ("Measure", "Commodity", "Date"). Change that to only contain date
    value2 = value2.apply(lambda x: (x[0][2], x[1]))

    #Store the data to be inserted into the database
    #This here is done only for exporting the data. We use value2 to plot the data
    #We need to split the (Date, Value) tuple into two columns
    export = value2.to_frame("Value")
    export["Date"] = export["Value"].apply(lambda x: x[0])
    export["Value"] = export["Value"].apply(lambda x: x[1])

    #Reorder value and date
    export = export.reindex(columns = ["Date", "Value"])

    data_list.append(export)

    for unit in ["$", "Tonnes"]:
        categories = value2[unit].reset_index()["Commodity"]

        fig, ax = plt.subplots(figsize=(9.6,6.2))

        # Set the new bounds
        left, bottom, width, height = ax.get_position().bounds
        new_left = (max([len(s) for s in categories]) + 1)*0.009
        new_width = width - (new_left - left)
        ax.set_position([new_left, bottom, new_width, height])

        values = value2.apply(lambda x: x[1])
        
        bars = ax.barh(categories, values[unit], color=["#3498DB", "#5DADE2"])

        for i, rect in enumerate(bars):
            ax.text(rect.get_x() + rect.get_width(), rect.get_y() + rect.get_height()/2, s = value2[unit][i][0].strftime('%d-%m-%Y'), va="center",ha="left")
        ax.set_xlabel("Value")
        ax.set_ylabel("Commodity")
        ax.set_title("Day with the biggest value in " + unit + " per commodity")

    #==================================================================================

    appfunc()