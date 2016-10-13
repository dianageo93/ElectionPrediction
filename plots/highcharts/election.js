$(document).ready(function() {
    console.log('ok');
    $.ajax({
        type: "GET",
        url: "aggregated_election.csv",
        dataType: "text",
        success: function(data) {processElectionData(data);}
     });
});

function processElectionData(e) {
    console.log('ok2');
    var csv = $.csv.toObjects(e, {
        separator: ","
    });
    var republicansElectionEvents = [];
    var democratsElectionEvents = [];
    csv.forEach(function (obj) {
        var evt = {
            x: Date.parse(obj.Date),
            y: 15,
            z: 15
        };
        if (obj.Party == 'republicans') {
            republicansElectionEvents.push(evt);
        }
        else if (obj.Party == 'democrats') {
            democratsElectionEvents.push(evt);
        }
        else {
            console.log('Uknown party: ' + evt.Party);
        }
    });

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
        }
    ];

    $('#bubble_chart').highcharts({

        chart: {
            type: 'bubble',
            plotBorderWidth: 1,
            zoomType: 'xy'
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
            headerFormat: '<table>',
            pointFormat: '<tr><th colspan="2"><h3>{point.country}</h3></th></tr>' +
                '<tr><th>Fat intake:</th><td>{point.x}g</td></tr>' +
                '<tr><th>Sugar intake:</th><td>{point.y}g</td></tr>' +
                '<tr><th>Obesity (adults):</th><td>{point.z}%</td></tr>',
            footerFormat: '</table>',
            followPointer: true
        },

        //plotOptions: {
        //    series: {
        //        dataLabels: {
        //            enabled: true,
        //            format: '{point.name}'
        //        }
        //    }
        //},

        series: bubbleChartSeries
    });
};
