var spikes;

$(document).ready(function() {
    $.ajax({
        type: "GET",
        url: "aggregated_election.csv",
        dataType: "text",
        success: function(data) {processElectionData(data);}
     });
    $.ajax({
        type: "GET",
        url: "spikes-300.json",
        dataType: "json",
        success: function(data) {processSpikes(data);}
     });
});

function processElectionData(e) {
    var csv = $.csv.toObjects(e, {
        separator: ","
    });
    var republicansElectionEvents = [];
    var democratsElectionEvents = [];
    var mixedElectionEvents = [];
    uniqueEvents = {}
    csv.forEach(function (obj) {
        var date = Date.parse(obj.Date);
        var newTooltip = obj.Party + ' ' + obj.Type + ' in ' + obj.Location + '<br>';
        if (date in uniqueEvents) {
            var evt = uniqueEvents[date];
        }
        else {
            var evt = {
                x: date,
                y: 15,
                z: 15,
                tooltip: '',
                parties: new Set()
            }
            uniqueEvents[date] = evt;
        }
        evt.tooltip += newTooltip;
        evt.parties.add(obj.Party);
    });

    for (date in uniqueEvents) {
        var evt = uniqueEvents[date];
        if (evt.parties.size == 1 && evt.parties.keys().next().value == 'democrats') {
            democratsElectionEvents.push(evt);
        }
        else if (evt.parties.size == 1 && evt.parties.keys().next().value == 'republicans') {
            republicansElectionEvents.push(evt);
        }
        else {
            mixedElectionEvents.push(evt);
        }
    }

    var bubbleChartSeries = [
        {
            name: 'Republicans',
            data: republicansElectionEvents,
            color: '#D50000'
        },
        {
            label: 'Democrats',
            data: democratsElectionEvents,
            color: '#283593'
        },
        {
            label: 'Combined events',
            data: mixedElectionEvents,
            color: '#9C27B0'
        }
    ];

    $('#bubble_chart').highcharts({

        chart: {
            type: 'bubble',
            plotBorderWidth: 1,
            panning: true
        },

        legend: {
            enabled: false
        },

        title: {
            text: 'Election events'
        },

        xAxis: {
            gridLineWidth: 1,
            //labels: {
            //    format: '{value} gr'
            //},
            type: 'datetime'
        },

        yAxis: {
            startOnTick: false,
            endOnTick: false,
            title: {
                text: 'No. of spiking topics'
            },
            maxPadding: 0.2
        },

        tooltip: {
            useHTML: true,
            headerFormat: '<div>',
            pointFormat: '{point.tooltip}',
            footerFormat: '</div>',
            followPointer: false
        },

        plotOptions: {
            series: {
                point: {
                    events: {
                        click: function (e) {
                            console.log(e.point.category);
                        }
                    }
                }
            }
        },

        mapNavigation: {
            enabled: true,
        },

        series: bubbleChartSeries
    });
}

function spikesCmp(a, b) {
    if (a.event_timestamp < b.event_timestamp)
        return -1;
    else if (a.event_timestamp > b.event_timestamp)
        return 1;
    else if (a.total_views < b.total_views)
        return -1;
    else if (a.total_views > b.total_views)
        return 1;
    else
        return 0;
}

function processSpikes(spikesArray) {
    for (var i = 0; i < spikesArray.length; i++) {
        var spike = spikesArray[i];
        var totalViews = 0;
        var days = ['two_days_before', 'one_day_before', 'on_the_day', 'one_day_after', 'two_days_after'];
        for (var j = 0; j < days.length; j++) {
            totalViews += spike[days[j]].reduce(function (a, b) {return a + b}, 0);
        }
        spike.total_views = totalViews;
    }
    spikes = spikesArray;
    spikes.sort(spikesCmp);
    console.log('done');
}

function bisectLeft(target) {
    var start = 0;
    var end = spikes.length;

    while (start < end) {
        mid = start + (end - start) / 2;
        if (spikes[mid].event_timestamp < target)
            start = mid + 1;
        else
            end = mid;
    }
    return start;
}

function bisectRight(target) {
    var start = 0;
    var end = spikes.length;

    while (start < end) {
        mid = start + (end - start) / 2;
        if (target < spikes[mid].event_timestamp)
            end = mid;
        else
            start = mid + 1;
    }
    return start;
}

function doPlots() {
    $("#spark_lines tr").remove();
    for (var i = 0; i < 3; i++) {
        var row = $('<tr></tr>');
        row.append('<td>Matza mu</td>');
        var sparkLine = $('<td></td>');
        row.append(sparkLine);
        $('#spark_lines').append(row);
        plotSparkline(sparkLine);
    }
}

function plotSparkline(el, data) {
    el.highcharts({
        chart: {
            type: 'area',
            borderWidth: 0,
            margin: [2, 0, 2, 0],
            width: 120,
            height: 20,
            style: {
                overflow: 'visible'
            },
            skipClone: true
        },
        credits: {enabled: false},
        exporting: {enabled: false},
        title: {
            text: ''
        },
        xAxis: {
            labels: {
                enabled: false
            },
            title: {
                text: null
            },
            startOnTick: false,
            endOnTick: false,
            tickPositions: []
        },
        yAxis: {
            endOnTick: false,
            startOnTick: false,
            labels: {
                enabled: false
            },
            title: {
                text: null
            },
            tickPositions: [0]
        },
        legend: {
            enabled: false
        },
        tooltip: {
            enabled: false
        },
        plotOptions: {
            series: {
                animation: false,
                lineWidth: 1,
                shadow: false,
                states: {
                    hover: {
                        lineWidth: 1
                    }
                },
                marker: {
                    radius: 1,
                    states: {
                        hover: {
                            radius: 2
                        }
                    }
                },
                fillOpacity: 0.25
            },
            column: {
                negativeColor: '#910000',
                borderColor: 'silver'
            }
        },
        series: [{
            name: 'USA',
            data: [null, null, null, null, null, 6, 11, 32, 110, 235, 369, 640,
                1005, 1436, 2063, 3057, 4618, 6444, 9822, 15468, 20434, 24126,
                27387, 29459, 31056, 31982, 32040, 31233, 29224, 27342, 26662,
                26956, 27912, 28999, 28965, 27826, 25579, 25722, 24826, 24605,
                24304, 23464, 23708, 24099, 24357, 24237, 24401, 24344, 23586,
                22380, 21004, 17287, 14747, 13076, 12555, 12144, 11009, 10950,
                10871, 10824, 10577, 10527, 10475, 10421, 10358, 10295, 10104]
        }, {
            name: 'USSR/Russia',
            data: [null, null, null, null, null, null, null, null, null, null,
                5, 25, 50, 120, 150, 200, 426, 660, 869, 1060, 1605, 2471, 3322,
                4238, 5221, 6129, 7089, 8339, 9399, 10538, 11643, 13092, 14478,
                15915, 17385, 19055, 21205, 23044, 25393, 27935, 30062, 32049,
                33952, 35804, 37431, 39197, 45000, 43000, 41000, 39000, 37000,
                35000, 33000, 31000, 29000, 27000, 25000, 24000, 23000, 22000,
                21000, 20000, 19000, 18000, 18000, 17000, 16000]
        }]
    });
}
