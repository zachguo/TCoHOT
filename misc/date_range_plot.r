# interactive R to create plot from trending data
# Created by Trevor Edelblute, Mar 2014.

mydata <- read.csv("datefreq.csv", header=TRUE, sep",")

attach(mydata)

plot(year, freq, xaxp = c(1500, 2000, 20))

