import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import json
import sys
import os

figindex = 0

data = []

show = bool(sys.argv[2]) if len(sys.argv) > 2 else False

filename, file_extension = os.path.splitext(sys.argv[1])

pp = PdfPages(filename+".pdf")

with open(sys.argv[1]) as f:
    c = json.load(f)
    experiments = c['experiments']
    stageData = {}
    for experiment in experiments:
        stages = experiment['stages']
        for stageId,stage in stages.items():
            if not stageId in stageData:
                stageData[stageId] = (stageId, [], [], [])
            durations = list(map(float, filter(lambda x: x > 0, stage['task_durations'])))
            #records = stage['actual_records_read'] / len(durations)
            #t_records = [float(y)/float(records) for y in durations]

            stageData[stageId][1].extend(durations)
            stageData[stageId][2].append(stage['s_GQ_ta_master'])
            stageData[stageId][3].append(stage['io_factor'])

    data = sorted(list(stageData.values()), key=lambda o: int(o[0]))

def main():

    res = {}

    fig = plt.figure(figsize=(6,6))

    for x in data:
        stageid = x[0]
        measurements = np.array(x[1])

        gqs = x[2]
        ios = x[3]

        printSigma(measurements, stageid)

        mean=np.mean(measurements)
        std=np.std(measurements)

        gqmean = np.mean(gqs)
        iomean = np.mean(ios)

        gqstd = np.std(gqs)
        iostd = np.std(ios)

        res[stageid] = {
            'gq':gqmean,
            'io':iomean,
            't_record_mean': mean,
            't_record_std': std
        }

    fig.canvas.mpl_connect('key_press_event', lambda event: onKeyPressed(event, fig))


    if show:
        switchFig(fig, 0)
        plt.show()
    else:
        for i in range(0, len(data)):
            switchFig(fig, i)

def printSigma(measurements, stageid):

    mean = np.mean(measurements)
    std = np.std(measurements)
    l = float(len(measurements))

    s0=float(len(list(filter(lambda x: x <= mean+0*std, measurements))))/l
    s025=len(list(filter(lambda x: x <=mean+0.25*std, measurements)))/l
    s05=len(list(filter(lambda x: x <=mean+0.5*std, measurements)))/l
    s1=len(list(filter(lambda x: x <= mean+1*std, measurements)))/l
    s2=len(list(filter(lambda x: x <= mean+2*std, measurements)))/l
    s3=len(list(filter(lambda x: x <= mean+3*std, measurements)))/l
    s4=len(list(filter(lambda x: x <= mean+4*std, measurements)))/l
    s5=len(list(filter(lambda x: x <= mean+5*std, measurements)))/l
    s6=len(list(filter(lambda x: x <= mean+6*std, measurements)))/l

    print("Stage "+stageid)

    print("sigma0.00: "+ str(s0))
    print("sigma0.25: "+ str(s025))
    print("sigma0.50: "+ str(s05))
    print("sigma1.00: "+ str(s1))
    print("sigma2.00: "+ str(s2))
    print("sigma3.00: "+ str(s3))
    print("sigma4.00: "+ str(s4))
    print("sigma5.00: "+ str(s5))
    print("sigma6.00: "+ str(s6))

def onKeyPressed(event, fig):
    global figindex
    if event.key == 'right':
        figindex += 1
        figindex %= len(data)
    elif event.key == 'left':
        figindex = figindex = len(data)-1 if figindex == 0 else figindex-1
    fig.clear()
    switchFig(fig, figindex)
    plt.draw()


def switchFig(fig, i):

    measurements = np.array(data[i][1])

    # 1 bin = 10 points
    bins = int(len(measurements)*0.1)

    stageid = data[i][0]

    mean = np.mean(measurements)
    std = np.std(measurements)

    xs = np.linspace(mean-4*std,mean+4*std,bins)
    normFit = mlab.normpdf(xs, mean, std)

    ax = fig.add_subplot(111)
    ax.plot(xs, normFit,  'r--', linewidth=1)
    n, _, patches = ax.hist(measurements, bins-1, density=1, facecolor='green', alpha=0.75)

    h = np.max(n)+np.max(n)/4

    ax.axvline(x=mean)
    ax.text(mean+mean/80, h, "sigma0", rotation=90, verticalalignment='center', weight='bold')
    ax.axvline(x=mean+1*std)
    ax.text(mean+1*std+mean/80, h, "sigma1", rotation=90, verticalalignment='center', weight='bold')
    ax.axvline(x=mean+2*std)
    ax.text(mean+2*std+mean/80, h, "sigma2", rotation=90, verticalalignment='center', weight='bold')
    ax.axvline(x=mean+3*std)
    ax.text(mean+3*std+mean/80, h, "sigma3", rotation=90, verticalalignment='center', weight='bold')
    ax.set_xlabel('Data')
    ax.set_ylabel('Probability')
    ax.set_title("Histogram of Stage "+stageid)
    ax.grid(True)
    ax.axis([mean-4*std, mean+4*std, 0, np.max(n)+np.max(n)/2])
    #ax.tick_params(labelsize=-1, colors='w')

    if not show:
        pp.savefig(fig)
        fig.clear()
    #ax2 = fig.add_subplot(122)
    #stats.probplot(measurements, dist=stats.norm,  sparams=(2,20), plot=ax2)

main()
pp.close()



#KDEpdf = gaussian_kde(measurements)
#esty = KDEpdf(xs)
#fig = plt.figure()
#ax.set_title(title)
#plt.plot(estx,esty,'r',label="KDE estimation",color="blue")
#plt.plot(estx[:bins],KDEpdf.resample(size=bins)[0],label="KDE returns",  color="red")
#plt.plot(estx[:bins], np.random.choice(x, bins),label="KDE returns",  color="blue")
#import scipy.stats as stats
#from scipy.stats.kde import gaussian_kde
#from scipy.stats import norm
