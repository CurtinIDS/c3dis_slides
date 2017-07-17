
# coding: utf-8

# In[1]:


import ipywidgets
from ipywidgets import Label, Layout
import IPython


# In[2]:


class FilteringWidget:
    
    def __init__(self, dataframe, columns_of_interest):
        self.dataframe = dataframe
        self.columns_of_interest = columns_of_interest 
        self.orig_df = self.dataframe
        self.hboxes = []
        self.widget_column = {}
        self.button = ipywidgets.Button(description="Reset")
        self.button.on_click(self.reset)
        IPython.display.display(self.button)
        self.build_widgets()
    
    def reset(self, change):   
        self.dataframe = self.orig_df.copy()
        self.build_widgets()
    
    def some_change(self, change):
        if change['type'] == 'change' and change['name'] == 'value':
            try:
                self.dataframe = self.dataframe[self.dataframe[self.widget_column[change.owner]] == change['new'][0]]
            except:
                try:
                    self.dataframe = self.dataframe[self.dataframe[self.widget_column[change.owner]] == int(change['new'][0])]
                except:
                    raise
            self.build_widgets()


    def build_widgets(self):
        for hbox in self.hboxes:
            hbox.close()
        hboxes = []
        for column in self.columns_of_interest:
            uniques = self.dataframe[column].unique()
            hbox = ipywidgets.HBox()
            label = Label(
                value=column + " (" + str(len(uniques)) + ")",
                layout=Layout(width='50%')
                         )
            a_widget = ipywidgets.SelectMultiple(
                options=list(uniques),
                rows=10,
                disabled=False
            )
            self.widget_column[a_widget] = column
            hbox.children = (label, a_widget)
            a_widget.observe(self.some_change)
            self.hboxes.append(hbox)
            IPython.display.display(hbox)    

