from flask import Flask, redirect, url_for, render_template ,request
import os
import np
import requests
import time
import  werkzeug


app = Flask(__name__)

def formcheck(templatename):
    try:
        if request.method == "POST":
            searchterm = request.form["searchterm"]
            sites = request.form["sites"]
            pageno = request.form["pageNo"]

            if (sites == "limet"):
                import scrapelt
                return render_template("results.html", stuff=scrapelt.main(searchterm, pageno).to_html(index=False,
                                                                                                       classes="table display  table-striped table-bordered table-sm",
                                                                                                       render_links=True,
                                                                                                       escape=False),
                                       searchquery=searchterm)
            if (sites == "1337"):
                import scrapeleetx
                return render_template("results.html", stuff=scrapeleetx.main(searchterm, pageno).to_html(index=False,
                                                                                                          classes="table display table-striped table-bordered table-sm",
                                                                                                          render_links=True,
                                                                                                          escape=False),
                                       searchquery=searchterm)
            if (sites == "katcr"):
                import scrapekat
                return render_template("results.html", stuff=scrapekat.main(searchterm, pageno).to_html(index=False,
                                                                                                        classes="table display table-striped table-bordered table-sm",
                                                                                                        render_links=True,
                                                                                                        escape=False),
                                       searchquery=searchterm)
        else:
            return render_template(templatename)
    except werkzeug.exceptions.BadRequestKeyError:
        return render_template(templatename)


@app.route("/",methods=["GET","POST"])
def home():
     return formcheck("index.html")

@app.route("/",methods=["GET","POST"])
def results():
    return formcheck("results.html")

@app.route("/output")
def output():
    return render_template("results.html")

if (__name__ == "__main__"):
    app.run(debug=False)
