################
#K-Means
################
testImageS = read.csv("c:/Users/smoeller/Documents/MSCS/CS5542/Tutorial 3/CS5542-Lab3/src/imageS.csv")
x<-rbind(testImageS$Q1_saturation,testImageS$Q2_saturation,testImageS$Q3_saturation,testImageS$Q4_saturation)
x<-t(x)  #this is the transpose function
km <- kmeans(x, 3, 25)
print(km)
plot(x, col = km$cluster)



##############
#K-Medians
##############
install.packages("flexclust")
library(flexclust)
imageDF <- testImageS[,-1]
cl2 = kcca(imageDF, k=4, family=kccaFamily("kmedians"), control=list(initcent="kmeanspp"))
image(cl2)
points(imageDF)



##############
#EM
##############
install.packages("EMCluster")
library(EMCluster)
imageDF <- testImageS[,-1]
ret.1 <- starts.via.svd(imageDF, nclass = 10, method = "em")
summary(ret.1)
plotem(ret.1, imageDF)



#############
#Hierarchical Clustering 
#############
d <- dist(as.matrix(testImageS))
hc <- hclust(d)
plot(hc)
