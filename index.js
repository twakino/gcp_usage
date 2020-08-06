/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
const BOT_ACCESS_TOKEN = 'xoxb-XXXXXXXXXXXXXXXXxxx';
const CHANNEL = '#bill_gcp';
const request = require('request');
const slack = require('slack');
const dateformat = require('dateformat');
exports.billingNotifer = (event, context) => {
  const { BigQuery } = require('@google-cloud/bigquery');
  const bigquery = new BigQuery({
    projectId: 'kuraseru-client'
  });
  //query
  var query = 'SELECT format_timestamp(\'%Y/%m\', usage_start_time, \'Asia/Tokyo\') as mon, ' +
  'project.name as project, service.description as service, round(sum(cost * 100)) / 100 as cost ' +
  'FROM kuraseru-client.billing_export.gcp_billing_export_v1_01682F_16656C_7372F9 ' +
  'where format_timestamp(\'%Y/%m\', usage_start_time, \'Asia/Tokyo\') = format_timestamp(\'%Y/%m\', current_timestamp, \'Asia/Tokyo\') ' +
  'GROUP BY mon, project, service order by project';
   
  var options = {
      query: query,
      location:"asia-northeast1",
      useLegacySql: false
  };
   
  var bqresult = "";
  var project_name = "";
  var cnt = 0;
  var subtotal = 0;
  var total = 0;
 
  //query結果をproject毎にまとめる
  bigquery.createQueryJob(options)
    .then(results => {
        const [job] = results;
        return job.getQueryResults();
    })
    .then(async results => {
        const [rows] = results;
        for (var i=0; i<rows.length; i++) {
            var row = rows[i];
            if (cnt == 0) {
                project_name = row["project"];
                cnt = 1;
            }
            //projectとprojectの間には罫線を入れる
            if (project_name != row["project"]) {
                bqresult += "----------\n";
                bqresult += "小計 $" + (Math.floor(subtotal * Math.pow(10, 2)) / Math.pow(10, 2)) + "\n\n";
                subtotal = 0;
            }
            bqresult += row["project"] + "\t" + row["service"] + "\t$" + row["cost"] + "\n";
            subtotal += row["cost"];
            total += row["cost"];
            project_name = row["project"];
        }
        bqresult += "----------\n";
        bqresult += "小計 $" + (Math.floor(subtotal * Math.pow(10, 2)) / Math.pow(10, 2)) + "\n\n";
        bqresult += "合計 $" + (Math.floor(total * Math.pow(10, 2)) / Math.pow(10, 2)) + "\n"; 
         
        var dt = new Date();
        var fd = dateformat(dt,"yyyy-mm-dd");
        //slackに投稿   
          await slack.chat.postMessage({
            token: BOT_ACCESS_TOKEN,
            channel: CHANNEL,
            text: "今月のGCPコスト (as of " + fd + ")\n" + bqresult,
            // text: JSON.stringify(rows, hoge())
          });
    })
    .catch(error => {
        console.log(error);
    })
};
