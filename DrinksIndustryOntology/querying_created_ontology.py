# Importing necessary Modules
import rdflib

# Creating an empty RDF graph and then parsing the generated ontology into it.
g = rdflib.Graph()
g.parse("drink_industry_dbpedia_wikidata_ontology.owl", "xml")

print("graph has %s statements.\n" % len(g))


# Querying from Coffee Companies sub-class to get companies name with their revenue arranged in descending order
coffee_houses_query = """
PREFIX di: <http://www.semanticweb.org/ontologies/ECS7028P/drinks_industry.owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  
SELECT DISTINCT ?companyName ?revenue
WHERE { ?Coffeehouse rdf:type di:Coffeehouse .
        ?Coffeehouse di:companyName ?companyName .
        ?Coffeehouse di:revenue ?revenue
      }ORDER BY DESC(?revenue)"""

# Displaying the coffee_houses_query result in nicely format a table
print("Displaying the query_coffee_houses result in nicely format a table")
print('{0:30s} {1:20s}'.format("CompanyName", "Revenue($)"))
for CompanyName, Revenue in g.query(coffee_houses_query):
    print('{0:30s} {1:20s}'.format(CompanyName, Revenue))


# Querying from breweries sub-class to get companies name alongwith the Country of their headquarters,
# listed in more than one stock exchange
brewery_query = """
PREFIX di: <http://www.semanticweb.org/ontologies/ECS7028P/drinks_industry.owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  
SELECT DISTINCT ?companyName (COUNT(?stockExchange) AS ?numOfStockExchangeListing) ?country
WHERE { ?brewery rdf:type di:brewery .
        ?brewery di:companyName ?companyName .
        ?brewery di:stockExchange ?stockExchange .
        ?brewery di:hasHeadquartersInCountry ?country .
      }
GROUP BY ?companyName
HAVING (COUNT(?stockExchange)>1)"""

# Displaying the brewery_query result in nicely format a table
print("\nDisplaying the brewery_query result in nicely format a table")
print('{0:15s} {1:30s} {2:60s}'.format("CompanyName", "NumOfStockExchangeListing", "Country"))
for CompanyName, NumOfStockExchangeListing, Country in g.query(brewery_query):
    print('{0:15s} {1:30s} {2:60s}'.format(CompanyName, NumOfStockExchangeListing, Country))


# Querying from the combined Ontology to filter the coffee companies and breweries- companies name, establishment year
# number of employees working and company's offical web-site which where established before 2000 and 
# have a head count equal to or greater than 100, arranged in descending order of the employees count.
combined_query = """
PREFIX di: <http://www.semanticweb.org/ontologies/ECS7028P/drinks_industry.owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  
SELECT DISTINCT ?companyName ?foundingYear ?numberOfEmployees ?homePage
WHERE { ?Industry rdf:type ?type .
        FILTER (?type IN (di:Coffeehouse, di:brewery))
        
        ?Industry di:companyName ?companyName .
        ?Industry di:foundingYear ?foundingYear .
        ?Industry di:numberOfEmployees ?numberOfEmployees .
        ?Industry di:homePageURL ?homePage .
        
        FILTER (?foundingYear < 2003)
        FILTER (?numberOfEmployees >= 100)
        
      }ORDER BY DESC(?numberOfEmployees)"""

# Displaying the combined_query result in nicely format a table
print("\nDisplaying the combined_query result in nicely format a table")
print('{0:30s} {1:15s} {2:15s} {3:50s}'.format("CompanyName", "FoundingYear", "NumOfEmployees", "HomePage"))
for CompanyName, FoundingYear, NumOfEmployees, HomePage in g.query(combined_query):
    print('{0:30s} {1:15s} {2:15s} {3:50s}'.format(CompanyName, FoundingYear, NumOfEmployees, HomePage))
