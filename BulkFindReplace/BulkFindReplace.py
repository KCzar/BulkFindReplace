# -*- coding: utf-8 -*-
"""
Created on Sun Oct 12 13:08:12 2014

@author: Ken
"""

import marisa_trie
import re
import pandas as pd
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
    def __init__(self, trieInput, version="v4"):
        if isinstance(trieInput, basestring):
            trieInput = pd.read_csv(trieInput)
        self.frTrie = marisa_trie.Trie(list(trieInput.iloc[:, 0].apply(unicode)))
        self.frDict = dict(zip(trieInput.iloc[:, 0].apply(unicode), trieInput.iloc[:, 1].apply(unicode)))
        self.startRegex = re.compile(r'[^\w]')
        self.endRegex = re.compile(r'[^\w]')
        self.BulkFindReplace_str = self.BulkFindReplace_orig_str
        if version == "v3":
            self.BulkFindReplace_str = self.BulkFindReplace_v3_str
        elif version == "v4":
            self.BulkFindReplace_str = self.BulkFindReplace_v4_str


    def BulkFindReplace_orig_str(self, inString, startRegex=r'[^\w]', endRegex=r'[^\w]'):
        i = 0
        outString = inString
        strLen = len(outString)
        while (i < strLen):
            if i is 0 or re.search(startRegex, outString[i - 1]):
                remainingStr = outString[i:]
                pref_list = self.frTrie.prefixes(remainingStr)
                if len(pref_list) > 0:
                    # iterate backwards through list
                    for pref in pref_list[::-1]:
                        # make sure char after prefix is an endRegex char
                        if (len(remainingStr) is len(pref) or re.search(endRegex, remainingStr[len(pref)])):
                            # if there is a valid prefix, replace 1st instance
                            mapStr = self.frDict[pref]
                            if mapStr != pref:
                                outString = outString[:i] + remainingStr.replace(pref, mapStr, 1)
                                strLen = len(outString)
                            i += len(mapStr) - 1
                            break
            i += 1
        return outString


    def BulkFindReplace_v3_str(self, inString, startRegex=r'[^\w]', endRegex=r'[^\w]'):
        i = 0
        outString = inString
        strLen = len(outString)
        while (i < strLen):
            if i is 0 or self.startRegex.search(outString[i - 1]):
                remainingStr = outString[i:]
                pref_list = self.frTrie.prefixes(remainingStr)
                if len(pref_list) > 0:
                    # iterate backwards through list
                    for pref in pref_list[::-1]:
                        # make sure char after prefix is an endRegex char
                        if (len(remainingStr) is len(pref) or self.endRegex.search(remainingStr[len(pref)])):
                            # if there is a valid prefix, replace 1st instance
                            mapStr = self.frDict[pref]
                            if mapStr != pref:
                                outString = outString[:i] + remainingStr.replace(pref, mapStr, 1)
                                strLen = len(outString)
                            i += len(mapStr) - 1
                            break
            i += 1
        return outString



    def BulkFindReplace_v4_str(self, inString, startRegex=r'[^\w]', endRegex=r'[^\w]'):
        i = 0
        outString = inString
        outString_list = []
        #  while (i < strLen):
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
                        if (len(remainingStr) is len(pref) or self.endRegex.search(remainingStr[len(pref)])):
                            # if there is a valid prefix, replace 1st instance
                            mapStr = self.frDict[pref]
                            if mapStr != pref:
                                addStr = inString[lastCut:i] + mapStr
                                outString_list.append(addStr)
                                lastCut = i + len(pref)
                                # outString = outString[:i] + remainingStr.replace(pref, mapStr, 1)
                                # strLen = len(outString)
                            iSkipTo = i + len(pref)
                            break

        if len(outString_list) > 0:
            if lastCut < len(inString):
                outString_list.append(inString[lastCut:len(inString)])
            outString = "".join(outString_list)
        else:
            outString = inString
        return outString



    def BulkFindReplaceToCompletion_str(self, inString, startRegex=r'[^\w]', endRegex=r'[^\w]', maxCycles=10):
        cycle = 0
        previousStr = inString
        inString = self.BulkFindReplace_str(inString, startRegex, endRegex)
        cycle = 1

        if inString == previousStr or cycle >= maxCycles:
            return inString

        # Save secondToLastStr to help prevent endless cycles
        secondToLastStr = previousStr
        previousStr = inString
        inString = self.BulkFindReplace_str(inString, startRegex, endRegex)
        cycle = 2

        while (inString != previousStr and inString != secondToLastStr and cycle < maxCycles):
            secondToLastStr = previousStr
            previousStr = inString
            inString = self.BulkFindReplace_str(inString, startRegex, endRegex)
            cycle += 1

        # if cycle is 10:
        #     return "\nsecondToLastStr: " + secondToLastStr + ";\npreviousStr:     " + previousStr + ";\ncurrentStr:      " + inString + ";\n"

        return inString


    def BulkFindReplace(self, strSeries, startRegex=r'[^\w]', endRegex=r'[^\w]', maxCycles=10):
        isBaseString = isinstance(strSeries, basestring)
        strSeries = pd.Series(strSeries).copy()
        strSeries = strSeries.apply(unicode)
        strSeries = strSeries.apply(self.BulkFindReplaceToCompletion_str, (startRegex, endRegex, maxCycles))
        if isBaseString:
            return strSeries.iloc[0]
        return strSeries


    def BulkFindReplaceMPHelper(self, args):
        strSeries, startRegex, endRegex, maxCycles = args
        strSeries = strSeries.apply(self.BulkFindReplaceToCompletion_str, (startRegex, endRegex, maxCycles))
        return strSeries


    def BulkFindReplaceMultiProc(self, strSeries, startRegex=r'[^\w]', endRegex=r'[^\w]', maxCycles=10, workers=-1):
        isBaseString = isinstance(strSeries, basestring)
        strSeries = pd.Series(strSeries).copy()
        strSeries = strSeries.fillna("")
        strSeries = strSeries.apply(unicode)
        if workers == -1:
            if cpu_count() % 2 == 0:
                workers = int(cpu_count()/2)
            else:
                workers = cpu_count()
        if workers > 1:
            pool = Pool(processes=workers)
            strSeries_list = pool.map(self.BulkFindReplaceMPHelper, [(d, startRegex, endRegex, maxCycles) for d in np.array_split(strSeries, workers)])
            pool.close()
            strSeries = pd.concat(strSeries_list)
        else:
            strSeries = strSeries.apply(self.BulkFindReplaceToCompletion_str, (startRegex, endRegex, maxCycles))
        if isBaseString:
            return strSeries.iloc[0]
        return strSeries
