from tableau_scraper import TableauScraper

ts = TableauScraper()
ts.loads("https://public.tableau.com/views/Contributionpatient/Tableaudebord5?:showVizHome=no")

wb = ts.getWorkbook()

print(wb.getWorksheetNames())
