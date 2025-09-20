async function refresh(){
  const r = await fetch("/api/jobs");
  const data = await r.json();
  for (const job of data.jobs){
    const row = document.getElementById("job-"+job.id);
    if(!row) continue;
    row.querySelector(".status").textContent = job.status;
    row.querySelector(".progbar").style.width = job.progress.toFixed(1) + "%";
    row.querySelector(".progval").textContent = job.progress.toFixed(1) + "%";
  }
}
setInterval(refresh, 1500);
