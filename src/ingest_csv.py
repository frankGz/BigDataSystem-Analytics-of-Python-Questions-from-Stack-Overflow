from subprocess import Popen

create_hdfs_path = ['hdfs', 'dfs', '-mkdir', '-p', '/app/Documents/EECS4415_Project/data']
put2hdfs = ['hdfs', 'dfs', '-put', '-f', '/app/Documents/EECS4415_Project/data/Answers.csv', '/app/Documents/EECS4415_Project/data/Tags.csv', '/app/Documents/EECS4415_Project/data']

t = Popen(create_hdfs_path)
t.wait()
t = Popen(put2hdfs)
t.wait()