# spannerimport
command line application that imports a csv file into a cloud spanner table

# Setup

1 Create a cloud spanner table using the SiteRankings.ddl file provided in this repository

2 Open a cloud shell from the google cloud platform console

3 Clone this repository by entering git clone http:...

4 cd spannerimport

5 Set the application default login
     gcloud auth application-default login
     Enter 'Y' when prompted to continue
     Paste the URL into a browser tab
     Copy the verification code from the browser and paste it in the prompt

6 Enter the following to import the 50,000 row csv file into the SiteRankings table
    python sites.py --instance_id=[your spanner intance] --database_id=hn-[your spanner database] --table_id=SiteRankings --       batchsize=1600 --data_file=50000.csv --format_file=sites.fmt
    
