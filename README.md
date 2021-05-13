# Semantic-Similarity-Classification
A map-reduce application using a Google Syntactic N-Grams dataset, Amazon EMR and Hadoop map-reduce to calculate the co-occurrence vector
of each word pair in a given gold standard dataset, based on the various measures of association with
context and vector similarity discribed in the paper: https://www.cs.bgu.ac.il/~dsp211/wiki.files/04588492.pdf <br>
Then we can build a classifier based on these vectors, running classification algorithm in WEKA software: http://www.cs.waikato.ac.nz/ml/weka/index.html in order to classify word pairs by their semantic similarity.

The input is the English All - Biarcs dataset of Google Syntactic N-Grams: http://storage.googleapis.com/books/syntactic-ngrams/index.html, which provides syntactic parsing of Google-books N-Grams.
The format of the corpus is described in the README file.
