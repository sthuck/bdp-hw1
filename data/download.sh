#requires kaggle-cli
kaggle datasets download -d yelp-dataset/yelp-dataset
unzip yelp-dataset.zip
rm -v yelp-dataset.zip