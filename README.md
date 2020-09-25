# social-media-graph-clusters
Detecting communities in Social media graphs

1)Created a social media graph, by connecting Users who have rated similar businesses(adjusting similarity threshold to manage number of edges).
2)Implemented the Girvan Neuman algorithm in Python/PySpark, to find the betweenness values for each edge in this large social media graph(906 edges). 
3)Used the betweenness values to perform hierarchical clustering of the graph into tightly-knit communities, with the aim of maximising Modularity(measure of similarity between nodes in the same cluster).
