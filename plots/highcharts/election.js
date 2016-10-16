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
                            plotSparklines(e.point.category);
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
        return 1;
    else if (a.total_views > b.total_views)
        return -1;
    else
        return 0;
}

function processSpikes(spikesArray) {
    for (var i = 0; i < spikesArray.length; i++) {
        var spike = spikesArray[i];
        var totalViews = 0;
        var plotLines = [];
        var days = ['two_days_before', 'one_day_before', 'on_the_day', 'one_day_after', 'two_days_after'];
        var views = [];
        for (var j = 0; j < days.length; j++) {
            if (j > 0) {
                plotLines.push({
                    color: 'rgba(255, 84, 84, 0.5)',
                    value: views.length,
                    width: 1
                });
            }
            views = views.concat(spike[days[j]]);
            delete spike[days[j]];
        }
        totalViews += views.reduce(function (a, b) {return a + b}, 0);
        spike.total_views = totalViews;
        spike.views = views;
        spike.plot_lines = plotLines;
    }
    spikes = spikesArray;
    spikes.sort(spikesCmp);
}

function bisectLeft(target) {
    var start = 0;
    var end = spikes.length;

    while (start < end) {
        var mid = start + Math.floor((end - start) / 2);
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
        var mid = start + Math.floor((end - start) / 2);
        if (target < spikes[mid].event_timestamp)
            end = mid;
        else
            start = mid + 1;
    }
    return start;
}

function plotSparklines(timeStamp) {
    var date = new Date(timeStamp);
    date.setHours(7); // CST time zone
    var target = date.getTime();

    var start = bisectLeft(target);
    var end = bisectRight(target);
    end = end - start > 50 ? start + 50 : end;

    $("#spark_lines tr").remove();

    for (var i = start; i < end; i++) {
        var row = $('<tr></tr>');
        row.append('<td>' + spikes[i].topic + '</td>');
        var sparkLine = $('<td></td>');
        row.append(sparkLine);
        $('#spark_lines').append(row);
        plotSparkline(sparkLine, spikes[i]);
    }
}

function plotSparkline(el, data) {
    el.highcharts({
        chart: {
            type: 'area',
            borderWidth: 0,
            margin: [2, 0, 2, 0],
            width: 240,
            height: 40,
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
            tickPositions: [],
            plotLines: data.plot_lines
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
            },
            line: {
                marker: {
                    enabled: false
                }
            }
        },
        series: [{
            name: 'views',
            data: data.views
        }]
    });
}
