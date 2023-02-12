const apiHost = ""

function getPreviousDay(date = new Date()) {
    const previous = new Date(date.getTime());
    previous.setDate(date.getDate() - 1);
    return previous;
}

const datasetExchange = document.getElementById("dataset-exchange");
const datasetPair = document.getElementById("dataset-pair");
const datasetDate = document.getElementById("dataset-date");
datasetDate.max = getPreviousDay().toISOString().split('T')[0];
datasetDate.value = datasetDate.max;
const chartElement = document.getElementById("chart");
let chart = null;

let currentDataset = null;

function submit() {
    lookup(datasetExchange.value, datasetPair.value, datasetDate.value);
}
document.getElementById("dataset-submit").onclick = submit;
[datasetPair, datasetDate].forEach(e => e.addEventListener("keyup", function(event) {
    if (event.key === "Enter") {
        submit();
    }
}));

function afterSetExtremes(e) {
    const { chart } = e.target;
    // let hours = Math.floor(Math.abs(e.min - e.max) / 1000 / 60 / 60);
    // chart.series[0].setData(dedupeDataset(zoomDedupeDelay(hours)));
}

function filterDataset(dataset) {
    let buyDataset = [];
    let sellDataset = [];
    dataset.forEach(entry => {
        switch (entry[2]) {
            case "buy":
                buyDataset.push(entry);
                break;
            case "sell":
                sellDataset.push(entry);
                break;
            default:
                console.log("invalid side: " + entry);
                break;
        }
    });
    return [buyDataset, sellDataset];
}

async function lookup(exchange, coinPair, date) {
    chartElement.innerHTML = "<h1>loding</h1>";
    let response = await fetch(apiHost + "/dataset/" + encodeURIComponent(exchange) + "/"
        + encodeURIComponent(coinPair) + "/" + encodeURIComponent(date));
    if (response.status / 100 != 2) {
        let errMessage = await response.json().then(j => j.error);
        chartElement.innerText = "blą∂: " + errMessage;
        return;
    }
    currentDataset = await response.json();
    // po dacie sortowanie bo ten olx zajebany to fikolka jakiegos robi Xd
    currentDataset.sort(function(a, b) {
      return a[0] - b[0];
    });

    chartElement.innerHTML = "<h1>wyświetlam wykres..</h1>";
    // yield to update dom
    await new Promise(resolve => setTimeout(resolve, 0));
    let [buyDataset, sellDataset] = filterDataset(currentDataset);

    chart = Highcharts.stockChart('chart', {
        chart: { zoomType: 'x' },
        navigator: {
            adaptToUpdatedData: false,
            series: {
                data: buyDataset,
            }
        },
        scrollbar: { liveRedraw: false },
        title: {
            text: coinPair,
            align: 'left'
        },
        subtitle: {
            text: 'od zera do klasy sredniej',
            align: 'left'
        },
        rangeSelector: {
            buttons: [
                { type: 'minute', count: 1, text: '1m' },
                { type: 'minute', count: 2, text: '2m' },
                { type: 'minute', count: 3, text: '3m' },
                { type: 'minute', count: 10, text: '10m' },
                { type: 'hour', count: 1, text: '1h' },
                { type: 'hour', count: 2, text: '2h' },
                { type: 'hour', count: 6,text: '6h'},
                { type: 'hour', count: 12, text: '12h' },
                { type: 'all', text: 'All' },
            ],
            inputEnabled: false,
            selected: 8,
        },
        xAxis: {
            events: {
                afterSetExtremes: afterSetExtremes,
            },
            type: 'datetime'
        },
        legend: {
            enabled: true,
        },
        series: [
            {
                name: "buy",
                data: buyDataset,
                dataGrouping: {
                    enabled: false
                }
            },
            {
                name: "sell",
                data: sellDataset,
                dataGrouping: {
                    enabled: false
                }
            },
        ],
    });
}

Highcharts.theme = {
    colors: ['#8087E8', '#A3EDBA', '#F19E53', '#6699A1',
        '#E1D369', '#87B4E7', '#DA6D85', '#BBBAC5'],
    chart: {
        backgroundColor: '#121212',
    },
    title: {
        style: {
            fontSize: '22px',
            fontWeight: '500',
            color: '#fff'
        }
    },
    subtitle: {
        style: {
            fontSize: '16px',
            fontWeight: '400',
            color: '#fff'
        }
    },
    credits: {
        style: {
            color: '#f0f0f0'
        }
    },
    caption: {
        style: {
            color: '#f0f0f0'
        }
    },
    tooltip: {
        borderWidth: 0,
        backgroundColor: '#f0f0f0',
        shadow: true
    },
    legend: {
        backgroundColor: 'transparent',
        itemStyle: {
            fontWeight: '400',
            fontSize: '12px',
            color: '#fff'
        },
        itemHoverStyle: {
            fontWeight: '700',
            color: '#fff'
        }
    },
    labels: {
        style: {
            color: '#707073'
        }
    },
    plotOptions: {
        series: {
            dataLabels: {
                color: '#46465C',
                style: {
                    fontSize: '13px'
                }
            },
            marker: {
                lineColor: '#333'
            }
        },
        boxplot: {
            fillColor: '#505053'
        },
        candlestick: {
            lineColor: null,
            upColor: '#DA6D85',
            upLineColor: '#DA6D85'
        },
        errorbar: {
            color: 'white'
        },
        dumbbell: {
            lowColor: '#f0f0f0'
        },
        map: {
            borderColor: 'rgba(200, 200, 200, 1)',
            nullColor: '#78758C'

        }
    },
    drilldown: {
        activeAxisLabelStyle: {
            color: '#F0F0F3'
        },
        activeDataLabelStyle: {
            color: '#F0F0F3'
        },
        drillUpButton: {
            theme: {
                fill: '#fff'
            }
        }
    },
    xAxis: {
        gridLineColor: '#707073',
        labels: {
            style: {
                color: '#fff',
                fontSize: '12px'
            }
        },
        lineColor: '#707073',
        minorGridLineColor: '#505053',
        tickColor: '#707073',
        title: {
            style: {
                color: '#fff'
            }
        }
    },
    yAxis: {
        gridLineColor: '#707073',
        labels: {
            style: {
                color: '#fff',
                fontSize: '12px'
            }
        },
        lineColor: '#707073',
        minorGridLineColor: '#505053',
        tickColor: '#707073',
        tickWidth: 1,
        title: {
            style: {
                color: '#fff',
                fontWeight: '300'
            }
        }
    },
    // scroll charts
    rangeSelector: {
        buttonTheme: {
            fill: '#46465C',
            stroke: '#BBBAC5',
            'stroke-width': 1,
            style: {
                color: '#fff'
            },
            states: {
                hover: {
                    fill: '#1f1836',
                    style: {
                        color: '#fff'
                    },
                    'stroke-width': 1,
                    stroke: 'white'
                },
                select: {
                    fill: '#1f1836',
                    style: {
                        color: '#fff'
                    },
                    'stroke-width': 1,
                    stroke: 'white'
                }
            }
        },
        inputBoxBorderColor: '#BBBAC5',
        inputStyle: {
            backgroundColor: '#2F2B38',
            color: '#fff'
        },
        labelStyle: {
            color: '#fff'
        }
    },
    navigator: {
        handles: {
            backgroundColor: '#BBBAC5',
            borderColor: '#2F2B38'
        },
        outlineColor: '#CCC',
        maskFill: 'rgba(255,255,255,0.1)',
        series: {
            color: '#A3EDBA',
            lineColor: '#A3EDBA'
        },
        xAxis: {
            gridLineColor: '#505053'
        }
    },
    scrollbar: {
        barBackgroundColor: '#333',
        barBorderColor: '#333',
        buttonArrowColor: '#fff',
        buttonBackgroundColor: '#333',
        buttonBorderColor: '#333',
        rifleColor: '#fff',
        trackBackgroundColor: '#000',
        trackBorderColor: '#000'
    }
};
Highcharts.setOptions(Highcharts.theme);
