import sys
import src.jobs.EmpJob as e
import src.jobs.DeptJob as d

if __name__ == "__main__":
    jobName =  sys.argv[0]
    if jobName == 'empJob':
        e.empRun()
    elif jobName == 'deptJob':
        d.deptRun()

