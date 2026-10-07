from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent
im = Image.new('RGB', (1800, 1120), '#0b1220')
d = ImageDraw.Draw(im)
font_path = 'C:/Windows/Fonts/segoeui.ttf'
bold_path = 'C:/Windows/Fonts/segoeuib.ttf'
def text(x, y, value, size=24, color='#cbd5e1', bold=False):
    d.text((x, y), value, font=ImageFont.truetype(bold_path if bold else font_path, size), fill=color)
def box(x, y, w, h, title, lines, accent='#38bdf8', planned=False):
    d.rounded_rectangle((x,y,x+w,y+h), radius=18, fill='#131f31', outline=accent, width=2)
    text(x+22,y+18,title,27,accent,True)
    for n,line in enumerate(lines):
        text(x+22,y+65+n*33,line,22)
    if planned:
        text(x+w-128,y+20,'PLANNED',16,'#94a3b8',True)
def arrow(a,b,color='#38bdf8'):
    d.line((a,b),fill=color,width=4)
    x,y=b
    d.polygon([(x,y),(x-13,y-8),(x-13,y+8)],fill=color)
text(60,35,'FLUX / CURRENT ARCHITECTURE',44,'#f8fafc',True)
text(60,97,'Music listening data, from public dumps to analytical marts',26)
text(60,144,'Repository snapshot: 2026-10-07 | Solid paths: implemented | Gray paths: planned',21,'#94a3b8')
box(60,230,320,180,'01 / SOURCES',['ListenBrainz dumps','Daily incremental batches','Checksum + resume'])
box(435,230,335,180,'02 / BRONZE',['raw_listens','Original JSON retained','Dump-scoped replacement'],'#d6a06a')
box(825,230,365,180,'03 / SILVER',['listens_normalized','MBID resolution + dedup','Quality rules'],'#cbd5e1')
box(1245,230,495,180,'04 / GOLD',['fact_listens','dim_artist / SCD Type 2','dim_date + dim_user'],'#fbbf24')
arrow((380,315),(435,315)); arrow((770,315),(825,315)); arrow((1190,315),(1245,315))
box(825,460,365,150,'QUALITY QUARANTINE',['invalid_listens','Rejected rows + reasons'],'#cbd5e1')
d.line(((1007,410),(1007,460)),fill='#cbd5e1',width=3)
box(1245,460,495,150,'ANALYTICAL MARTS',['mart_daily_artist','mart_rising_artist / weekly top 20'],'#fbbf24')
d.line(((1492,410),(1492,460)),fill='#fbbf24',width=3)
box(60,460,710,150,'METADATA PATH',['MusicBrainz -> artist normalization -> dim_artist','Silver users -> dim_user | fact date range -> dim_date'])
text(60,638,'EXECUTION & STORAGE',24,'#f8fafc',True)
box(60,680,535,180,'SPARK BATCH',['CLI scripts / local mode or standalone','Compose: master + 3 workers','Shared file storage / Delta Lake'])
box(635,680,555,180,'OBSERVABILITY & VALIDATION',['ops/job_runs / success + failure','Spark event logs + measurement reports','Pytest, DuckDB oracle, failure drills'])
box(1245,680,495,160,'SERVING',['PostgreSQL serving instance configured','Mart export + FastAPI API pending'],'#64748b',True)
d.line(((1492,610),(1492,680)),fill='#64748b',width=3)
box(60,890,1130,140,'ORCHESTRATION',['Airflow infrastructure + smoke DAG exist','Production pipeline DAG integration is pending'],'#64748b',True)
text(1245,909,'SCOPE',25,'#f8fafc',True)
text(1245,953,'Batch MVP. Kafka and streaming',22)
text(1245,986,'are deferred to Phase 2.',22)
text(60,1060,'Data-flow view: arrows show logical dependencies, not network connections.',21,'#94a3b8')
im.save(ROOT/'architecture.png')


