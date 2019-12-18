import numpy as np
import matplotlib.pyplot as plt
import matplotlib

def print_overview_statistics(overview_pd, gender):
	print("DISTRIBUTION FOR "+gender+" OVERVIEWS:")
	print("The distribution's minimum is {0:.2f}%".format(np.min(overview_pd['adjective_ratio_overview']*100)))
	print("The distribution's maximum is {0:.2f}%".format(np.max(overview_pd['adjective_ratio_overview']*100)))
	print("The distribution's mean    is {0:.2f}%".format(np.mean(overview_pd['adjective_ratio_overview']*100)))
	print("The distribution's median  is {0:.2f}%\n".format(np.median(overview_pd['adjective_ratio_overview']*100)))

def plot_overview_distribution(ratio_adj_overview_male_pd, ratio_adj_overview_fem_pd):
	# Plot the histogram of the feature
	fig, ax = plt.subplots(1,2,figsize=(15,5))
	ax[0].hist(ratio_adj_overview_male_pd['adjective_ratio_overview'], bins = 15, color = 'green', alpha=0.5)
	ax[1].hist(ratio_adj_overview_fem_pd['adjective_ratio_overview'], bins = 15, color = 'red', alpha=0.5)

	ax[0].set_title('Distribution Percentage of adjectives\n in the biography overview - Male')
	ax[0].set_xlabel('Percentage')
	ax[0].set_ylabel('Frequency')
	ax[1].set_title('Distribution Percentage of adjectives\n in the biography overview - Female')
	ax[1].set_xlabel('Percentage')
	ax[1].set_ylabel('Frequency')

	ax[0].set_yscale('log')
	ax[1].set_yscale('log')

def print_overview_len(overview_len_pd):
	print("The distribution's minimum is {0:.2f}".format(np.min(overview_len_pd['overview_len'])))
	print("The distribution's maximum is {0:.2f}".format(np.max(overview_len_pd['overview_len'])))
	print("The distribution's mean    is {0:.2f}".format(np.mean(overview_len_pd['overview_len'])))
	print("The distribution's median  is {0:.2f}".format(np.median(overview_len_pd['overview_len'])))

def plot_overview_len(overview_len_pd):
	pl_male = overview_len_pd['overview_len'].plot(kind="hist", figsize=(7, 5), log=True,\
                                               alpha=0.5, color=["green"], bins=20)
	pl_male.set_title('Distribution of overview\'s length')
	pl_male.set_xlabel('Length')
	pl_male.set_ylabel('Frequency')
	plt.show()