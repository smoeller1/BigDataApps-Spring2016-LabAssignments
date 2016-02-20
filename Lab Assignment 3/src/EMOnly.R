#Load mocked up CSV data
#This data is meant to represent the color hue of the 4 primary quadrants of each image
testImageS = read.csv("c:/Users/smoeller/Documents/MSCS/CS5542/Tutorial 3/CS5542-Lab3/src/imageS.csv")

#Install the EMCluster package
install.packages("EMCluster")

#Load the EMCluster library
library(EMCluster)

#Strip the record labels (column 1) from the data set
imageDF <- testImageS[,-1]

#Run EM calculation
ret.1 <- starts.via.svd(imageDF, nclass = 10, method = "em")

#Summarize the results
summary(ret.1)

#Display a graphical plot of the results
plotem(ret.1, imageDF)