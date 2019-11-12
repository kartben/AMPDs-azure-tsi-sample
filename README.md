# AMPDs-azure-tsi-sample

This project allows to quickly demonstrate the capabilities of Azure Time Series Insights, by means of ingesting several millions of data points from the [AMPds dataset](http://ampds.org/). The AMPds dataset contains electricity, water, and natural gas measurements at one minute intervals — a total of 1,051,200 readings per meter (21 power meters total) for 2 years of monitoring of an actual household. The dataset also includes hourly weather data that covers the same period of time.

In the context of this example, only power consumption and weather data are being used.

## Pre-requisites

* An Azure Event Hubs namespace -- see here TODO.

## License and Terms of Use for the AMPds2 dataset

The AMPds2 dataset is available under the terms of the [Creative Commons Attribution license (CC-BY)](https://creativecommons.org/licenses/by/4.0/).

> Makonin, S. et al. Electricity, water, and natural gas consumption of a residential house in Canada from 2012 to 2014. Sci. Data 3:160037 doi: 10.1038/sdata.2016.37 (2016).
