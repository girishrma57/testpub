#!/bin/bash

cat <<'EOF' > /tmp/cluster_monitor.sh
#!/bin/bash
cluster_id=$1

sleep 10s
logDbfsDir=/dbfs/cluster_termination_logs/
mkdir -p $logDbfsDir
 
stats_log="$logDbfsDir/${cluster_id}_termination_status_check.log"

sleep_interval=240

chauffeur_log="/databricks/chauffeur/logs/usage.json"
cluster_last_activity_metric="clusterLastActivityTime"

# set to threshold value of inactivity time in minutes.If the last
inactive_threshold_in_minutes="120"

# Set workspace URL with your region
workspace_url="https://westeurope.azuredatabricks.net/"

# Set it to token generated with https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-token
token="mytoken"

function terminate_cluster(){
 cluster_id=$1
 workspace_url=$2
 token=$3
 request_body="{\"cluster_id\":\"$cluster_id\"}:"
 curl -X POST "$workspace_url/api/2.0/clusters/delete" -H 'Content-Type: application/json' -H "Authorization: Bearer $token" -d "$request_body"
} 
 
while [ 1 ]; do
echo "Checking termination status at $(date +"%Y-%m-%d %H:%M:%S")" >> ${stats_log}
cluster_last_activity_time=` tail -n 100 $chauffeur_log  | grep -i '"metric":"chauffeurEvent"' | grep -i clusterLastActivityTime | tail -1 | grep -Eo '"clusterLastActivityTime":"([0-9]*)' | cut -d ":" -f2 | sed 's/"//g'`

if [ -z "$cluster_last_activity_time" ];then
  echo "Couldn't find cluster_last_activity_time metric from the log file" | awk -v date="$(date +"%Y-%m-%d %H:%M:%S") " '{print date $0}' >> ${stats_log}
else 
  current_epoch_time=`date +%s`
  time_diff_in_minutes=$(((current_epoch_time - cluster_last_activity_time/1000)/60))
  # if difference between current time and cluster_last_activity_time is greater than threshold value, terminate the cluster.
  if [ "$time_diff_in_minutes" -ge "$inactive_threshold_in_minutes" ]; then
    echo "Terminating the cluster.Inactive for $time_diff_in_minutes minutes.cluster_last_activity_time=`date -d @$((cluster_last_activity_time/1000))`" | awk -v date="$(date +"%Y-%m-%d %H:%M:%S") " '{print date - $0}' >> ${stats_log}
    terminate_cluster $cluster_id $workspace_url $token
  else 
     echo "Skipping termination since the cluster was active at `date -d @$((cluster_last_activity_time/1000))`" | awk -v date="$(date +"%Y-%m-%d %H:%M:%S") " '{print date - $0}' >> ${stats_log}
  fi
 fi
 sleep $sleep_interval
done

EOF


echo $DB_IS_DRIVER
if [[ $DB_IS_DRIVER = "TRUE" ]]; then
  chmod a+x /tmp/cluster_monitor.sh
  /tmp/cluster_monitor.sh ${DB_CLUSTER_ID}  & disown
fi
 