# plot final results
# Siyuan & Bin, Apr 2014

# load data
results <- read.csv("path/to/unweighted/results_text.csv")
results_w <- read.csv("path/to/weighted/results_text_w.csv")
results_date <- read.csv("path/to/unweighted/results_datetext.csv")
results_date_w <- read.csv("path/to/weighted/results_datetext_w.csv")

# plotting
library(ggplot2)
library(gridExtra)

se <- function(x) sd(x)/sqrt(length(x))

# one plot
getdf <- function(x, y, name){
    Means = c(sapply(x,function(x){mean(results[, x])}, simplify=T), 
              sapply(x,function(x){mean(results_w[, x])}, simplify=T))
    ses = c(sapply(x,function(x){se(results[, x])}, simplify=T), 
            sapply(x,function(x){se(results_w[, x])}, simplify=T))
    Metric = rep(y,2)
    Weight = c(rep(c('Unweighted'),length(x)), rep(c('Weighted'),length(x)))
    df_A <- data.frame(Means, ses, Metric, Weight)
    limits_A <- aes(ymax = Means + ses, ymin=Means - ses)
    p1 <- ggplot(df_A, aes(shape=Weight, colour=Weight, y=Means, x=Metric)) + 
        ylim(0,1) +
        geom_point(size=4) + 
        geom_errorbar(limits_A, width=0.5) + 
        labs(title=name, fill="", x=NULL, y='F-score') + 
        theme(text=element_text(size=25), legend.position="bottom")
    return(p1)
}

getdf_1 <- function(x, y, name){
    Means = c(sapply(x,function(x){mean(results_date[, x])}, simplify=T), 
              sapply(x,function(x){mean(results_date_w[, x])}, simplify=T))
    ses = c(sapply(x,function(x){se(results_date[, x])}, simplify=T), 
            sapply(x,function(x){se(results_date_w[, x])}, simplify=T))
    Metric = rep(y,2)
    Weight = c(rep(c('Unweighted'),length(x)), rep(c('Weighted'),length(x)))
    df_A <- data.frame(Means, ses, Metric, Weight)
    limits_A <- aes(ymax = Means + ses, ymin=Means - ses)
    p1 <- ggplot(df_A, aes(shape=Weight, color=Weight,y=Means, x=Metric)) + 
        ylim(0,1) +
        geom_point(size=4) + 
        geom_errorbar(limits_A, width=0.5) + 
        labs(title=name, fill="", x=NULL, y='F-score') + 
        theme(text=element_text(size=25), legend.position="bottom")
    return(p1)
}


# change here

p1 <- getdf(c('f_LR_nllr_o', 'f_LR_nllr_ou', 'f_LR_nllr_oub', 'f_LR_nllr_oubt'), c('o','ou','oub','oubt'),'LR + NLLR')
p5 <- getdf_1(c('f_LR_nllr_od', 'f_LR_nllr_oud', 'f_LR_nllr_oubd', 'f_LR_nllr_oubtd'), c('do','dou','doub','doubt'),'LR + NLLR')

p2 <- getdf(c('f_LR_cs_oubt','f_LR_kld_oubt', 'f_LR_nllr_oubt'), c('CS','KLD','NLLR'), 'LR')
p3 <- getdf(c('f_DT_cs_oubt','f_DT_kld_oubt', 'f_DT_nllr_oubt'), c('CS','KLD','NLLR'), 'DT')
p4 <- getdf(c('f_SVM_cs_oubt','f_SVM_kld_oubt', 'f_SVM_nllr_oubt'), c('CS','KLD','NLLR'), 'SVM')

p6 <- getdf_1(c('f_LR_cs_oubtd','f_LR_kld_oubtd', 'f_LR_nllr_oubtd'), c('CS','KLD','NLLR'), 'LR')
p7 <- getdf_1(c('f_DT_cs_oubtd','f_DT_kld_oubtd', 'f_DT_nllr_oubtd'), c('CS','KLD','NLLR'), 'DT')
p8 <- getdf_1(c('f_SVM_cs_oubtd','f_SVM_kld_oubtd', 'f_SVM_nllr_oubtd'), c('CS','KLD','NLLR'), 'SVM')

# pull out legend
g_legend<-function(a.gplot){
    tmp <- ggplot_gtable(ggplot_build(a.gplot))
    leg <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
    legend <- tmp$grobs[[leg]]
    return(legend)}
legend <- g_legend(p1)

# output file
png('compareMetric_oubt_blank.png',width=1800,height=900,units="px")
#bitmap("test_test.tiff", height = 10, width = 20, units = 'in', type="tiff24nc", res=300)

# layout
grid.arrange(arrangeGrob(p2 + theme(legend.position="none"),
                         p3 + theme(legend.position="none"),
                         p4 + theme(legend.position="none"),
                         p1 + theme(legend.position="none"),
                         p6 + theme(legend.position="none"),
                         p7 + theme(legend.position="none"),
                         p8 + theme(legend.position="none"),
                         p5 + theme(legend.position="none"),
                         #                          main ="this is a title",
                         nrow=2), 
             legend, 
             nrow=2,
             heights=c(10, 1))

dev.off()
