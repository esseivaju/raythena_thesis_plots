import matplotlib.pyplot as plt
import seaborn as sbs


def formatted_figure(figsize, titlesize, xlabel, ylabel, title, saveto, dpi, xticks, xlabels, legend,
                     plt_func, *args, **kwargs):
    sbs.set_palette("dark")
    plt.figure(figsize=figsize)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title, fontsize=titlesize)
    plt_func(*args, **kwargs)
    if xticks:
        plt.xticks(xticks, [""] + xlabels)
    if legend:
        plt.legend()
    plt.savefig(saveto, dpi=dpi)
