# Importing necessary Modules
import rdflib
from rdflib.graph import Graph, URIRef, Store
from SPARQLWrapper import SPARQLWrapper, XML
from rdflib import plugin


## Populating T-Box with Dbpedia
# Configuring dbpedia SPARQL querying end-point
dbpedia_sparql = SPARQLWrapper("http://dbpedia.org/sparql")

# Constructing query by making use of :
#
#   * PREFIX to bind prefixes in the query
#   * CONSTRUCT to build new individuals from the query
#   * WHERE to obtain data matching the conditions and patterns specified
#   * OPTIONAL to indicate that some property/object may not exist for each instances
#   * FILTER to apply certain restriction on the property, to exclude certain informations from it
#   * BIND to assign result of an expression defining a property/object to a new variable
#   * UNION to merge results from different property/object to get the distinct results from both 
#   * STR to convert literals/ URI in string
#   * isURI condition to check if property/object represents a URI
#   * xsd:double represents a XML Schema 'double' datatype
#
dbpedia_construct_query="""
    PREFIX di: <http://www.semanticweb.org/ontologies/ECS7028P/drinks_industry.owl#>
    PREFIX dbpo: <http://dbpedia.org/ontology/>
    PREFIX dbpr: <http://dbpedia.org/resource/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dbpp: <http://dbpedia.org/property/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    CONSTRUCT {
      ?Coffeehouse rdf:type di:Coffeehouse .
      ?Coffeehouse di:companyName ?companyName .
      ?Coffeehouse di:founderName ?founderName .
      ?Coffeehouse di:hasHeadquartersInCity ?hqLocationCity .
      ?hqLocationCity rdf:type di:hqLocationCity .
      ?Coffeehouse di:hasHeadquartersInCountry ?hqLocationCountry .
      ?hqLocationCountry rdf:type di:hqLocationCountry .
      ?Coffeehouse di:numberOfLocations ?numberOfLocations .
      ?Coffeehouse di:hasParentCompany ?parentCompany .
      ?parentCompany rdf:type di:parentCompany .
      ?Coffeehouse di:numberOfEmployees ?numberOfEmployees .
      ?Coffeehouse di:foundingYear ?foundingYear .
      ?Coffeehouse di:homePageURL ?homePageURL .
      ?Coffeehouse di:operatesIn ?areaServed .
      ?areaServed rdf:type di:areaServed .
      ?Coffeehouse di:offersProduct ?products .
      ?products rdf:type di:products .
      ?Coffeehouse di:revenue ?revenue .
      ?Coffeehouse di:hasSubsidiary ?subsidiary .
      ?subsidiary rdf:type di:subsidiary .
    }
    WHERE {
      ?Coffeehouse dbpo:industry dbpr:Coffeehouse .
      ?Coffeehouse foaf:name ?companyName . FILTER (STR(?companyName) != "")

      OPTIONAL {
        { ?Coffeehouse dbpp:founder ?founderName . FILTER(!isURI(?founderName))}
        UNION
        { ?Coffeehouse dbpp:founders ?founderName .}
      }
      OPTIONAL {
         { ?Coffeehouse dbpp:hqLocationCity ?hqLocationCity . FILTER(isURI(?hqLocationCity))}
         UNION
         { ?Coffeehouse dbpo:location ?hqLocationCity . FILTER(isURI(?hqLocationCity))}
      }
      OPTIONAL { ?Coffeehouse dbpp:hqLocationCountry ?hqLocationCountry . FILTER(isURI(?hqLocationCountry))}
      OPTIONAL { ?Coffeehouse dbpo:numberOfLocations ?numberOfLocations .}
      OPTIONAL { ?Coffeehouse dbpo:parentCompany ?parentCompany .}
      OPTIONAL { ?Coffeehouse dbpo:numberOfEmployees ?numberOfEmployees .}
      OPTIONAL { ?Coffeehouse dbpo:foundingYear ?foundingYear .}
      OPTIONAL { ?Coffeehouse dbpp:homepage ?homePageURL .}
      OPTIONAL { ?Coffeehouse dbpp:areaServed ?areaServed . FILTER(isURI(?areaServed))}
      OPTIONAL {
        { ?Coffeehouse dbpo:product ?products . FILTER(isURI(?products))}
        UNION
        { ?Coffeehouse dbpp:products ?products . FILTER(isURI(?products))}
      }
      OPTIONAL { ?Coffeehouse dbpo:revenue ?originalRevenue . BIND(xsd:double(?originalRevenue) AS ?revenue)}
      OPTIONAL { ?Coffeehouse dbpo:subsidiary ?subsidiary .}
    }"""

# Setting the above defined query for SPARQL endpoint
# Defining the return format of the query output
dbpedia_sparql.setQuery(dbpedia_construct_query)
dbpedia_sparql.setReturnFormat(XML)

# Creating the RDF store and graph instance
# Using rdflib modules to create a new graph and store it in memory (temporarily).
memory_store = plugin.get('Memory', Store)()
graph_id = URIRef('http://www.semanticweb.org/store/drinks_industry')
g = Graph(store = memory_store, identifier = graph_id)

print("It might take some time ...")

# Executing the query and storing the results as RDF
g = dbpedia_sparql.query().convert()
# Parsing the created T-box of drinks_basic_ontology as valid RDFlib graphs
g.parse("drinks_industry_basic_ontology.owl")

# Serializing the graph in new ontology file (XML format)
g.serialize("drink_industry_dbpedia_ontology.owl", "xml")

# Displaying Success Message
print("...Ontology populated with dbpedia data using a dbpedia SPARQL endpoint!")
print("")


## Populating T-Box with wikidata on top of Dbpedia
# Configuring wikidata SPARQL querying end-point
wikidata_sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

# Constructing query by making use of :
#
#   * PREFIX to bind prefixes in the query
#   * CONSTRUCT to build new individuals from the query
#   * WHERE to obtain data matching the conditions and patterns specified
#   * OPTIONAL to indicate that some property/object may not exist for each instances
#   * FILTER to apply certain restriction on the property, to exclude certain informations from it
#   * BIND to assign result of an expression defining a property/object to a new variable
#   * STRDT to create a specified datatype literals
#   * STR to convert literals/ URI in string
#   * URI to convert string to URI
#   * CONCAT to merge two or more strings in one string
#   * LANG to retrieve language tag of string literals
#   * xsd:gYear represents a XML Schema 'double' datatype
#
wikidata_construct_query="""
    PREFIX di: <http://www.semanticweb.org/ontologies/ECS7028P/drinks_industry.owl#>
    PREFIX wd: <http://www.wikidata.org/entity/>
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    prefix owl: <http://www.w3.org/2002/07/owl#>

    CONSTRUCT {
      ?newbrewery rdf:type di:brewery .
      ?newbrewery owl:sameAs ?brewery .
      ?newbrewery di:companyName ?companyName .
      ?newbrewery di:hasHeadquartersInCountry ?hqLocationCountry .
      ?hqLocationCountry owl:sameAs ?originalhqLocationCountry .
      ?hqLocationCountry rdf:type di:hqLocationCountry .
      ?newbrewery di:hasParentCompany ?parentCompany .
      ?parentCompany owl:sameAs ?originalparentCompany .
      ?parentCompany rdf:type di:parentCompany .
      ?newbrewery di:hasSubsidiary ?subsidiary .
      ?subsidiary owl:sameAs ?originalsubsidiary .
      ?subsidiary rdf:type di:subsidiary .
      ?newbrewery di:numberOfEmployees ?numberOfEmployees .
      ?newbrewery di:hasHeadquartersInCity ?hqLocationCity .
      ?hqLocationCity owl:sameAs ?originalhqLocationCity .
      ?hqLocationCity rdf:type di:hqLocationCity .
      ?newbrewery di:foundingYear ?foundingYear .
      ?newbrewery di:offersProduct ?products .
      ?products owl:sameAs ?originalproducts .
      ?products rdf:type di:products .
      ?newbrewery di:stockExchange ?stockExchange .
      ?newbrewery di:homePageURL ?homePageURL .
    }
    WHERE {
      ?brewery wdt:P452 wd:Q131734.
      ?brewery rdfs:label ?companyName. 
      FILTER (LANG(?companyName) = "en")
      BIND(URI(CONCAT(STR(di:), REPLACE(?companyName," ", "_"))) AS ?newbrewery)
      OPTIONAL {?brewery wdt:P17 ?originalhqLocationCountry.
                ?originalhqLocationCountry rdfs:label ?hqLocationCountryName.
                FILTER (LANG(?hqLocationCountryName) = "en")
                BIND(URI(CONCAT(STR(di:), REPLACE(?hqLocationCountryName," ", "_"))) AS ?hqLocationCountry)}
      OPTIONAL { ?brewery wdt:P749 ?originalparentCompany.
                 ?originalparentCompany rdfs:label ?originalparentCompanyName.
                 FILTER (LANG(?originalparentCompanyName) = "en")
                 BIND(URI(CONCAT(STR(di:), REPLACE(?originalparentCompanyName," ", "_"))) AS ?parentCompany)}
      OPTIONAL { ?brewery wdt:P355 ?originalsubsidiary.
                 ?originalsubsidiary rdfs:label ?originalsubsidiaryName.
                 FILTER (LANG(?originalsubsidiaryName) = "en")
                 BIND(URI(CONCAT(STR(di:), REPLACE(?originalsubsidiaryName," ", "_"))) AS ?subsidiary)}
      OPTIONAL { ?brewery wdt:P1128 ?numberOfEmployees .}
      OPTIONAL { ?brewery wdt:P159 ?originalhqLocationCity.
                 ?originalhqLocationCity rdfs:label ?originalhqLocationCityName.
                 FILTER (LANG(?originalhqLocationCityName) = "en")
                 BIND(URI(CONCAT(STR(di:), REPLACE(?originalhqLocationCityName," ", "_"))) AS ?hqLocationCity)}
      OPTIONAL { ?brewery wdt:P571 ?originalfoundingYear . BIND(STRDT(STR(year(?originalfoundingYear)), xsd:gYear) AS ?foundingYear)}
      OPTIONAL { ?brewery wdt:P1056 ?originalproducts.
                 ?originalproducts rdfs:label ?originalproductsName.
                 FILTER (LANG(?originalproductsName) = "en")
                 BIND(URI(CONCAT(STR(di:), REPLACE(?originalproductsName," ", "_"))) AS ?products)}
      OPTIONAL {{ ?brewery wdt:P414 ?stockExchangeName .}
               ?stockExchangeName rdfs:label ?stockExchange . FILTER (LANG(?stockExchange) = "en")}
      OPTIONAL { ?brewery wdt:P856 ?homePageURL .}
    }"""

# Setting the above defined query for SPARQL endpoint
# Defining the return format of the query output
wikidata_sparql.setQuery(wikidata_construct_query)
wikidata_sparql.setReturnFormat(XML)

# Creating the RDF store and graph instance
# Using rdflib modules to create a new graph and store it in memory (temporarily).
memory_store = plugin.get('Memory', Store)()
graph_id = URIRef('http://www.semanticweb.org/store/drinks_industry')
g = Graph(store = memory_store, identifier = graph_id)

print("It might take some time ...")

# Executing the query and storing the results as RDF
g = wikidata_sparql.query().convert()
# Parsing the drinks_basic ontology as valid RDFlib graphs
g.parse("drink_industry_dbpedia_ontology.owl")

# # Serializing the graph in new ontology file (XML format)
g.serialize("drink_industry_dbpedia_wikidata_ontology.owl", "xml")

# Displaying Success Message
print("...Ontology with dbpedia data populated with wikidata data using a wikidata SPARQL endpoint!")
print("")



