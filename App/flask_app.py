from flask import Flask, render_template, request
import pandas as pd
import os


app = Flask(__name__)
df = pd.read_csv("../parsed_file.csv")

@app.context_processor
def provide_symbols():
    df_symb = df["SYMBOL"].unique()
    return {'symbols': df_symb}

@app.route('/')
def search():
    return render_template("search.html")

@app.route("/result", methods=["POST"])
def result():
    symbol = request.form.get("symbol")
    df_res = df[(df.SYMBOL == symbol)]
    df_res["TIMESTAMP"]=pd.to_datetime(df_res["TIMESTAMP"])
    df_res.sort_values(by=["TIMESTAMP"], ascending=False, inplace=True)
    return render_template("result.html", rows=[df_res.to_html(classes='data', index=False)], header="true")

if __name__ == "__main__":
    app.run()
