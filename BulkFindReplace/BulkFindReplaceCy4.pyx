# -*- coding: utf-8 -*-
"""
Created on Sun Oct 12 13:08:12 2014

@author: Ken
"""

import marisa_trie
import re
import pandas as pd
cimport numpy as np
import numpy as np
import sys


if sys.version_info[0] == 3:
    basestring = str
    unicode = str

from multiprocessing import Pool, cpu_count

"""
testTrie = marisa_trie.Trie([u'derpx', u'derpy', u'derpz'])

testFRDict = {u'derpx': u'derp', u'derpy': u'derp', u'derpz': u'derp'}

trieInput_df = pd.DataFrame(data=testFRDict, index=["Values"]).T
trieInput_df["Keys"] = trieInput_df.index
trieInput_df = trieInput_df.ix[:, ["Keys", "Values"]]
"""



class BulkFindReplacer:
    def __init__(self, trieInput, startRegex=r'[^\w]', endRegex=r'[^\w]'):
        if isinstance(trieInput, basestring):
            trieInput = pd.read_csv(trieInput)
        self.frTrie = marisa_trie.Trie(list(trieInput.iloc[:, 0].apply(unicode)))
        self.frDict = dict(zip(trieInput.iloc[:, 0].apply(unicode), trieInput.iloc[:, 1].apply(unicode)))
        self.startRegex = re.compile(startRegex)
        self.endRegex = re.compile(endRegex)


    def setRegex_void(self, startRegex=None, endRegex=None):
        """
        Alter existing startRegex and endRegex expressions of BulkFindReplacer instance.
        Regex expressions must match exactly one character.
        """
        if startRegex:
            self.startRegex = re.compile(startRegex)
        if endRegex:
            self.endRegex = re.compile(endRegex)


    def BulkFindReplace_str(self, str inString):
        outString_list = []
        iSkipTo = -1
        lastCut = 0
        for i in [0] + [x.end() for x in self.startRegex.finditer(inString)]:
            if i >= iSkipTo:
                remainingStr = inString[i:]
                pref_list = self.frTrie.prefixes(remainingStr)
                if len(pref_list) > 0:
                    # iterate backwards through list
                    for pref in pref_list[::-1]:
                        # make sure char after prefix is an endRegex char
                        prefLen = len(pref)
                        if (len(remainingStr) == prefLen or self.endRegex.search(remainingStr[prefLen])):
                            # if there is a valid prefix, replace 1st instance
                            mapStr = self.frDict[pref]
                            if mapStr != pref:
                                addStr = inString[lastCut:i] + mapStr
                                outString_list.append(addStr)
                                lastCut = i + prefLen
                            iSkipTo = i + prefLen
                            break

        if lastCut > 0:
            if lastCut < len(inString):
                outString_list.append(inString[lastCut:])
            outString = "".join(outString_list)
        else:
            return inString
        return outString


    def BulkFindReplaceToCompletion_str(self, str inString, maxCycles=10):
        cycle = 0
        previousStr = inString
        inString = self.BulkFindReplace_str(inString)
        cycle = 1

        if inString == previousStr or cycle >= maxCycles:
            return inString

        # Save secondToLastStr to help prevent endless cycles
        secondToLastStr = previousStr
        previousStr = inString
        inString = self.BulkFindReplace_str(inString)
        cycle = 2

        while (inString != previousStr and inString != secondToLastStr and cycle < maxCycles):
            secondToLastStr = previousStr
            previousStr = inString
            inString = self.BulkFindReplace_str(inString)
            cycle += 1

        # if cycle is 10:
        #     return "\nsecondToLastStr: " + secondToLastStr + ";\npreviousStr:     " + previousStr + ";\ncurrentStr:      " + inString + ";\n"

        return inString


    def BulkFindReplace(self, strSeries, maxCycles=10):
        if isinstance(strSeries, basestring):
            return self.BulkFindReplaceToCompletion_str(strSeries, maxCycles)
        strSeries = pd.Series(strSeries).copy()

        cdef np.ndarray[str] strSeries_cArray = strSeries.values

        for i in xrange(len(strSeries_cArray)):
            strSeries_cArray[i] = self.BulkFindReplaceToCompletion_str(strSeries_cArray[i])

        strSeries = pd.Series(strSeries_cArray, index=strSeries.index)

        # strSeries = strSeries.apply(self.BulkFindReplaceToCompletion_str, (maxCycles))
        return strSeries


    def BulkFindReplaceMPHelper(self, args):
        strSeries, maxCycles = args
        strSeries = strSeries.apply(self.BulkFindReplaceToCompletion_str, (maxCycles))
        return strSeries


    def BulkFindReplaceMultiProc(self, strSeries, maxCycles=10, workers=-1):
        if isinstance(strSeries, basestring):
            return self.BulkFindReplaceToCompletion_str(strSeries, maxCycles)
        strSeries = pd.Series(strSeries, dtype=str).copy()
        strSeries = strSeries.fillna("")
        strSeries = strSeries.apply(unicode)
        if workers == -1:
            if cpu_count() % 2 == 0:
                workers = int(cpu_count()/2)
            else:
                workers = cpu_count()
        if workers > 1:
            pool = Pool(processes=workers)
            strSeries_list = pool.map(self.BulkFindReplaceMPHelper, [(d, maxCycles) for d in np.array_split(strSeries, workers)])
            pool.close()
            strSeries = pd.concat(strSeries_list)
        else:
            strSeries = strSeries.apply(self.BulkFindReplaceToCompletion_str, (maxCycles))
        return strSeries