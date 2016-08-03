# wikipedia-notebooks


It includes scripts to get information from the Wikipedia and Wikidata API's.
You can get:
* Official WIkipedia page titles
*  Corresponding Wikidata IDs
* Available language translations
* Word count of the Wikipedia page
* All the links (within Wikipedia) pointing to the page of interest
* Also, includes a script  that runs a PageRank algorithm to generate a ranking based on the popularity of each entry in Wikipedia (number of wikipedia links pointing to the item)

To run locally:
### Official WIkipedia page titles and word count
If you want to get the official Wikipedia page titles for entries of your database and their cprresponding word counts you need to run:
`Find_original_wikipedia_title_and_wordcount.py` in your terminal
This script takes two arguments, a csv with all the entries you want to find WIkipedia data for (needs to have a `name` column) and a filename.csv for the results to be saved to.
For example if your data is saved in a csv file called input_data.csv and you want the result to be saved in output_data.csv, in the terminal run:
```
python Find_original_wikipedia_title_and_wordcount.py input_data.csv output_data.csv
```
This will go through your data and find the Wikipedia titles and wordount that most closely match your data.

You might need to do a clean up process as some Wikipedia pages might not match your input 100%. An example of a clean up process is given as an `ipython notebook` file in the `Jupyter_notebooks_with_analysis`, `Find_original_wikipedia_title_and_wordcount.ipynb`. Given your data you might need to run additional clean up steps.


### Corresponding Wikidata IDs
Once you have the official Wikipedia titles you can find the corresponding Wikidata IDs. This process also runs backwards, if you have the Wikidata ID and you want to find the Wikipedia title. Run `Request_Wikipedia_Wikidata_from_API.py` in your terminal with your input_data file (.csv) and output_data file (.csv) as the two arguments. For example,
```
python Request_Wikipedia_Wikidata_from_API.py input_data.csv output_data.csv
```

### Available language translations
This script allows you to find all the localized names from Wikipedia for each of your entries. Takes as an input a csv file with a column `wk:page` where all the Wikipedia titles will be stored and a `id` or `wof:id` column with the unique identifiers. It returns two json files in a dictionary format (key: value pairs). The first output includes as keys the `wk:page` names and the localized names as values of every key. The second output includes as keys the ids (`wof:id` or just `id`) and the localized names as values of every key. To run for example:
```
python Find_all_languages_titles_from_Wiki.py input_data.csv output_data_names.json output_data_ids.json
```

### All the links (within Wikipedia) pointing to the page of interest (Linkshere)
To get all the Wikipedia page names that point to the Wikipedia page of interest run this script. It limits the results to 2000 as some Wikipedia pages might have multiple thousand results and it increases the processing time. Takes as an input a csv file with a column `wk:page` where all the Wikipedia titles will be stored. It returns a json file in a dictionary format (key: value pairs) with each name as keys and the names of the Wikipedia pages linking to it as values to every key. For example to run:
```
python Linkshere_data.py input_data.csv output_data.json
```

### Ranking of your entries
To generate a ranking of your entries based on their popularity in Wikipedia run this script. We measure popularity as the number of links pointing to that page which creates a graph of Wikipedia pages connectivity. Takes as an input the file that was generated from the `Linkshere.py` script, which is a json file in a dictionary format (key: value pairs) with each name as keys and the names of the Wikipedia pages linking to it as values to every key and it will write a json file where the keys will be the Wikipedia names and the associated page rank score for each as a value. For example to run:
```
python PageRank.py input_data.json output_data.json
```

If your input file has more than 10,000 entries in an interest of processing time and RAM memory constraints, I recommend spliting it into multiple jsons (with about 5,000 entries each) and then run:
```
python PageRank_large_file_subfiles.py input_data.json output_data.json
```
This genarates the same output as before but takes as an input argument a folder name where all your jsons are and supports this functionality.

Enjoy!
