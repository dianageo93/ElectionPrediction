var spikes;
var spinner;

$(document).ready(function() {
    var target = document.getElementById('bubble_chart')
    spinner = new Spinner().spin(target);
    $.ajax({
        type: "GET",
        url: "spikes-300-3.json",
        dataType: "json",
        success: function(data) {processSpikes(data);}
     });

    (function (H) {
        H.wrap(H.Chart.prototype, 'mapZoom', function (proceed) {
            // Highcharts
            var pick = H.pick,
                UNDEFINED;
            // arguments
            var howMuch = arguments[1],
                centerXArg = arguments[2],
                centerYArg = arguments[3],
                mouseX = arguments[4],
                mouseY = arguments[5];

            var chart = this,
                xAxis = chart.xAxis[0],
                xRange = xAxis.max - xAxis.min,
                centerX = pick(centerXArg, xAxis.min + xRange / 2),
                newXRange = xRange * howMuch,
                yAxis = chart.yAxis[0],
                yRange = yAxis.max - yAxis.min,
                centerY = pick(centerYArg, yAxis.min + yRange / 2),
                newYRange = yRange * howMuch,
                fixToX = mouseX ? ((mouseX - xAxis.pos) / xAxis.len) : 0.5,
                fixToY = mouseY ? ((mouseY - yAxis.pos) / yAxis.len) : 0.5,
                newXMin = centerX - newXRange * fixToX,
                newYMin = centerY - newYRange * fixToY,
                newExt = chart.fitToBox({
                    x: newXMin,
                    y: newYMin,
                    width: newXRange,
                    height: newYRange
                }, {
                    x: xAxis.dataMin,
                    y: yAxis.dataMin,
                    width: xAxis.dataMax - xAxis.dataMin,
                    height: yAxis.dataMax - yAxis.dataMin
                });

            // When mousewheel zooming, fix the point under the mouse
            if (mouseX) {
                xAxis.fixTo = [mouseX - xAxis.pos, centerXArg];
            }
            if (mouseY) {
                yAxis.fixTo = [mouseY - yAxis.pos, centerYArg];
            }

            // Zoom
            if (howMuch !== undefined) {
                //xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                //xAxis.setExtremes(newExt.x, newExt.x + newExt.width, false);
                if (newExt.x == xAxis.dataMin) {
                    // add bubble padding
                    xAxis.userMin = UNDEFINED;
                    xAxis.userMax = UNDEFINED;
                } else {
                    // set min and max
                    xAxis.userMin = newExt.x;
                    xAxis.userMax = newExt.x + newExt.width;
                }
                //yAxis.setExtremes(newExt.y, newExt.y + newExt.height, false);
                if (newExt.y == yAxis.dataMin) {
                    // add bubble padding
                    yAxis.userMin = UNDEFINED;
                    yAxis.userMax = UNDEFINED;
                } else {
                    // set min and max
                    yAxis.userMin = newExt.y;
                    yAxis.userMax = newExt.y + newExt.height;
                }
                //xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                // Reset zoom
            } else {
                xAxis.setExtremes(undefined, undefined, false);
                yAxis.setExtremes(undefined, undefined, false);
            }
            chart.redraw();
        });
    }(Highcharts));
});

function processElectionData(e) {
    var maxSpikesPerEvent = 0;
    var maxViewsPerEvent = 0;
    var spikesPerEvent = 1, viewsPerEvent = spikes[0].total_views;

    for (var i = 1; i < spikes.length; i++) {
        if (spikes[i].event_timestamp != spikes[i - 1].event_timestamp) {
            maxSpikesPerEvent = Math.max(maxSpikesPerEvent, spikesPerEvent);
            maxViewsPerEvent = Math.max(maxViewsPerEvent, viewsPerEvent);
            spikesPerEvent = 0;
            viewsPerEvent = 0;
        }
        spikesPerEvent += 1;
        viewsPerEvent += spikes[i].total_views;
    }

    maxSpikesPerEvent = Math.max(maxSpikesPerEvent, spikesPerEvent);
    maxViewsPerEvent = Math.max(maxViewsPerEvent, viewsPerEvent);

    var csv = $.csv.toObjects(e, {
        separator: ","
    });
    var republicansElectionEvents = [];
    var democratsElectionEvents = [];
    var mixedElectionEvents = [];
    uniqueEvents = {}
    csv.forEach(function (obj) {
        var date = new Date(Date.parse(obj.Date));
        date.setHours(7);
        date = date.getTime();
        var newTooltip = obj.Party + ' ' + obj.Type + ' in ' + obj.Location + '<br>';
        if (date in uniqueEvents) {
            var evt = uniqueEvents[date];
        }
        else {
            var start = bisectLeft(date);
            var end = bisectRight(date);
            var views = 0;
            for (var i = start; i < end; i++) {
               views += spikes[i].total_views;
            }

            var evt = {
                x: date,
                y: 5 + (end - start) / maxSpikesPerEvent * 60,
                z: 5 + views / maxViewsPerEvent * 15,
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

    spinner.stop();
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
                text: 'Spiking topics'
            },
            //maxPadding: 0.2
        },

        tooltip: {
            useHTML: true,
            headerFormat: '<div>',
            pointFormat: '{point.tooltip}',
            footerFormat: '</div>'
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
                    width: 1,
                    dashStyle: 'Dash',
                });
            }
            views = views.concat(spike[days[j]]);
            delete spike[days[j]];
        }
        totalViews += views.reduce(function (a, b) {return a + b}, 0);
        spike.total_views = totalViews;
        spike.views = views;
        spike.plot_lines = plotLines;
        spike.topic = spike.topic.replace(/_/g, ' ').replace(/\b\w/g, function(l){ return l.toUpperCase() });
    }
    spikes = spikesArray;
    spikes.sort(spikesCmp);

    $.ajax({
        type: "GET",
        url: "aggregated_election.csv",
        dataType: "text",
        success: function(data) {processElectionData(data);}
    });
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
    var start = bisectLeft(timeStamp);
    var end = bisectRight(timeStamp);
    end = end - start > 50 ? start + 50 : end;

    $("#spark_lines tr").remove();

    var localSpikes = spikes.slice(start, end);
    localSpikes.forEach(function (spike) {
        var row = $('<tr></tr>');
        row.append('<td>' + spike.topic + '</td>');
        var sparkLine = $('<td></td>');
        row.append(sparkLine);
        $('#spark_lines').append(row);
        plotSparkline(sparkLine, spike);
    });
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
