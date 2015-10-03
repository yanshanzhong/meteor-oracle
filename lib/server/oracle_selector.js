// The minimongo selector compiler!

// Terminology:
//  - a "selector" is the EJSON object representing a selector
//  - a "matcher" is its compiled form (whether a full OracleSelector
//    object or one of the component lambdas that matches parts of it)
//  - a "result object" is an object with a "result" field and maybe
//    distance and arrayIndices.
//  - a "branched value" is an object with a "value" field and maybe
//    "dontIterate" and "arrayIndices".
//  - a "document" is a top-level object that can be stored in a collection.
//  - a "lookup function" is a function that takes in a document and returns
//    an array of "branched values".
//  - a "branched matcher" maps from an array of branched values to a result
//    object.
//  - an "element matcher" maps from a single value to a bool.

// Main entry point.
//   var matcher = new OracleSelector({a: {$gt: 5}});
//   if (matcher.documentMatches({a: 7})) ...
OracleSelector = function (selector) {
  var self = this;
  // A set (object mapping string -> *) of all of the document paths looked
  // at by the selector. Also includes the empty string if it may look at any
  // path (eg, $where).
  self._paths = {};
  // Set to true if compilation finds a $near.
  self._hasGeoQuery = false;
  // Set to true if compilation finds a $where.
  self._hasWhere = false;
  // Set to false if compilation finds anything other than a simple equality or
  // one or more of '$gt', '$gte', '$lt', '$lte', '$ne', '$in', '$nin' used with
  // scalars as operands.
  self._isSimple = true;
  // Set to a dummy document which always matches this Matcher. Or set to null
  // if such document is too hard to find.
  self._matchingDocument = undefined;
  // A clone of the original selector. It may just be a function if the user
  // passed in a function; otherwise is definitely an object (eg, IDs are
  // translated into {_id: ID} first. Used by canBecomeTrueByModifier and
  // Sorter._useWithMatcher.
  self._selector = null;
  self._docMatcher = self._compileSelector(selector);
};

_.extend(OracleSelector.prototype, {
  hasGeoQuery: function () {
    return this._hasGeoQuery;
  },
  hasWhere: function () {
    return this._hasWhere;
  },
  isSimple: function () {
    return this._isSimple;
  },

  // Given a selector, return a function that takes one argument, a
  // document. It returns a result object.
  _compileSelector: function (selector, sqlParameters) {
    var self = this;

    // shorthand -- scalars match _id
    if (LocalCollection._selectorIsId(selector)) {
      self._selector = {_id: selector};
      self._recordPathUsed('_id');
      return "id = "+_prepareOperand(selector);
    }

    // protect against dangerous selectors.  falsey and {_id: falsey} are both
    // likely programmer error, and not what you want, particularly for
    // destructive operations.
    if (!selector || (('_id' in selector) && !selector._id)) {
      self._isSimple = false;
      return "1 = 0";
    }

    // Top level can't be an array or true or binary.
    if (typeof(selector) === 'boolean' || isArray(selector) ||
        EJSON.isBinary(selector))
      throw new Error("Invalid selector: " + selector);

    self._selector = EJSON.clone(selector);
    return compileDocumentSelector(selector, self, {isRoot: true});
  },
  _recordPathUsed: function (path) {
    this._paths[path] = true;
  },
  // Returns a list of key paths the given selector is looking for. It includes
  // the empty string if there is a $where.
  _getPaths: function () {
    return _.keys(this._paths);
  }
});

var _prepareOperand = function(operand) {
	  var ret = null;
	    if (typeof operand === 'string') {
	    	operand = operand.replace("'", "''");
	    	ret = "'"+operand+"'";
	    } else {
	    	ret = operand;
	    }
	    
	    return ret;
};


// Takes in a selector that could match a full document (eg, the original
// selector). Returns a function mapping document->result object.
//
// matcher is the Matcher object we are compiling.
//
// If this is the root document selector (ie, not wrapped in $and or the like),
// then isRoot is true. (This is used by $near.)
var compileDocumentSelector = function (docSelector, matcher, options) {
  options = options || {};
  var docMatchers = [];
  _.each(docSelector, function (subSelector, key) {
    if (key.substr(0, 1) === '$') {
      // Outer operators are either logical operators (they recurse back into
      // this function), or $where.
      if (!_.has(LOGICAL_OPERATORS, key))
        throw new Error("Unrecognized logical operator: " + key);
      matcher._isSimple = false;
      var logicalMatcher = LOGICAL_OPERATORS[key](subSelector, matcher,
              options.inElemMatch);
      if(logicalMatcher && logicalMatcher !== "") {
    	  docMatchers.push(logicalMatcher);
      }
    } else {
      // Record this path, but only if we aren't in an elemMatcher, since in an
      // elemMatch this is a path inside an object in an array, not in the doc
      // root.
      if (!options.inElemMatch)
        matcher._recordPathUsed(key);
      var valueMatcher =
        compileValueSelector(subSelector, matcher, options.isRoot);
      if(valueMatcher && valueMatcher !== "") {
    	  docMatchers.push(valueMatcher(key));
      }
    }
  });

  return andDocumentMatchers(docMatchers);
};

// Takes in a selector that could match a key-indexed value in a document; eg,
// {$gt: 5, $lt: 9}, or a regular expression, or any non-expression object (to
// indicate equality).  Returns a branched matcher: a function mapping
// [branched value]->result object.
var compileValueSelector = function (valueSelector, matcher, isRoot) {
  if (valueSelector instanceof RegExp) {
    matcher._isSimple = false;
    return convertElementMatcherToBranchedMatcher(
      regexpElementMatcher(valueSelector));
  } else if (isOperatorObject(valueSelector)) {
    return operatorBranchedMatcher(valueSelector, matcher, isRoot);
  } else {
    return equalityElementMatcher(valueSelector);
  }
};

// Given an element matcher (which evaluates a single value), returns a branched
// value (which evaluates the element matcher on all the branches and returns a
// more structured return value possibly including arrayIndices).
var convertElementMatcherToBranchedMatcher = function (
    elementMatcher, options) {
  options = options || {};
  return elementMatcher;
};

// Takes a RegExp object and returns an element matcher.
regexpElementMatcher = function (regexp) {
  return function (key) {
    if (regexp instanceof RegExp) {
    	 throw Error("Regular expression operant has to be a string (not RegExp object)");
    }

    return "REGEXP_LIKE("+key+", "+_prepareOperand(regexp)+")";
  };
};

//Takes something that is not an operator object and returns an element matcher
//for equality with that thing.
equalityElementMatcher = function (elementSelector) {
if (isOperatorObject(elementSelector))
 throw Error("Can't create equalityValueSelector for operator object");

// Special-case: null and undefined are equal (if you got undefined in there
// somewhere, or if you got it due to some branch being non-existent in the
// weird special case), even though they aren't with EJSON.equals.
if (elementSelector == null) {  // undefined or null
 return function (key) {
   return key + " IS NULL";  // undefined or null
 };
}

return function (key) {
 return key + " = " + _prepareOperand(elementSelector);
};
};

//Takes something that is not an operator object and returns an element matcher
//for equality with that thing.
inequalityElementMatcher = function (elementSelector) {
if (isOperatorObject(elementSelector))
 throw Error("Can't create equalityValueSelector for operator object");

// Special-case: null and undefined are equal (if you got undefined in there
// somewhere, or if you got it due to some branch being non-existent in the
// weird special case), even though they aren't with EJSON.equals.
if (elementSelector == null) {  // undefined or null
 return function (key) {
   return key + " IS NOT NULL";  // undefined or null
 };
}

return function (key) {
 return "(" + key + " <> " + _prepareOperand(elementSelector) + " OR " + key + " IS NULL" + ")";
};
};

// Takes an operator object (an object with $ keys) and returns a branched
// matcher for it.
var operatorBranchedMatcher = function (valueSelector, matcher, isRoot) {
  // Each valueSelector works separately on the various branches.  So one
  // operator can match one branch and another can match another branch.  This
  // is OK.

  var operatorMatchers = [];
  _.each(valueSelector, function (operand, operator) {
    // XXX we should actually implement $eq, which is new in 2.6
    var simpleRange = _.contains(['$lt', '$lte', '$gt', '$gte'], operator) &&
      _.isNumber(operand);
    var simpleInequality = operator === '$ne' && !_.isObject(operand);
    var simpleInclusion = _.contains(['$in', '$nin'], operator) &&
      _.isArray(operand) && !_.any(operand, _.isObject);

    if (! (operator === '$eq' || simpleRange ||
           simpleInclusion || simpleInequality)) {
      matcher._isSimple = false;
    }

    if (_.has(VALUE_OPERATORS, operator)) {
      operatorMatchers.push(
        VALUE_OPERATORS[operator](operand, valueSelector, matcher, isRoot));
    } else if (_.has(ELEMENT_OPERATORS, operator)) {
      var options = ELEMENT_OPERATORS[operator];
      operatorMatchers.push(
          options.compileElementSelector(
            operand, valueSelector, matcher));
    } else {
      throw new Error("Unrecognized operator: " + operator);
    }
  });

  return andBranchedMatchers(operatorMatchers);
};

var compileArrayOfDocumentSelectors = function (
    selectors, matcher, inElemMatch) {
  if (!isArray(selectors) || _.isEmpty(selectors))
    throw Error("$and/$or/$nor must be nonempty array");
  return _.map(selectors, function (subSelector) {
    if (!isPlainObject(subSelector))
      throw Error("$or/$and/$nor entries need to be full objects");
    return compileDocumentSelector(
      subSelector, matcher, {inElemMatch: inElemMatch});
  });
};

// Operators that appear at the top level of a document selector.
var LOGICAL_OPERATORS = {
  $and: function (subSelector, matcher, inElemMatch) {
    var matchers = compileArrayOfDocumentSelectors(
      subSelector, matcher, inElemMatch);
    return andDocumentMatchers(matchers);
  },

  $or: function (subSelector, matcher, inElemMatch) {
    var matchers = compileArrayOfDocumentSelectors(
      subSelector, matcher, inElemMatch);

    // Special case: if there is only one matcher, use it directly, *preserving*
    // any arrayIndices it returns.
    if (matchers.length === 1)
      return matchers[0];

    return orDocumentMatchers(matchers);
  },

  $nor: function (subSelector, matcher, inElemMatch) {
    var matchers = compileArrayOfDocumentSelectors(
      subSelector, matcher, inElemMatch);

    return "NOT("+orDocumentMatchers(matchers)+")";
  },

  $where: function (selectorValue, matcher) {
	  return selectorValue;
  },

  // This is just used as a comment in the query (in MongoDB, it also ends up in
  // query logs); it has no effect on the actual selection.
  $comment: function () {
      return "";
  }
};

// Returns a branched matcher that matches iff the given matcher does not.
// Note that this implicitly "deMorganizes" the wrapped function.  ie, it
// means that ALL branch values need to fail to match innerBranchedMatcher.
var invertBranchedMatcher = function (branchedMatcher) {
  return function (key) {
    var invertMe = branchedMatcher(key);
    // We explicitly choose to strip arrayIndices here: it doesn't make sense to
    // say "update the array element that does not match something", at least
    // in mongo-land.
    return "NOT("+invertMe+")";
  };
};

// Operators that (unlike LOGICAL_OPERATORS) pertain to individual paths in a
// document, but (unlike ELEMENT_OPERATORS) do not have a simple definition as
// "match each branched value independently and combine with
// convertElementMatcherToBranchedMatcher".
var VALUE_OPERATORS = {
  $not: function (operand, valueSelector, matcher) {
    return invertBranchedMatcher(compileValueSelector(operand, matcher));
  },
  $eq: function (operand) {
	    return convertElementMatcherToBranchedMatcher(
	      equalityElementMatcher(operand));
  },
  $ne: function (operand) {
	    return convertElementMatcherToBranchedMatcher(
	      inequalityElementMatcher(operand));
  },
  $exists: function (operand) {
    var exists = convertElementMatcherToBranchedMatcher(function (key) {
      return key + (operand ? " IS NOT NULL" : " IS NULL");
    });
    return exists;
  },
  // $options just provides options for $regex; its logic is inside $regex
  $options: function (operand, valueSelector) {
    if (!_.has(valueSelector, '$regex'))
      throw Error("$options needs a $regex");
    return "";
  },
  // $maxDistance is basically an argument to $near
  $maxDistance: function (operand, valueSelector) {
      throw Error("$maxDistance operator is not supported yet");
  },
  $all: function (operand, valueSelector, matcher) {
      throw Error("$all operator is not supported yet");
  },
  $near: function (operand, valueSelector, matcher, isRoot) {
      throw Error("$near operator is not supported yet");
  }
};

// Helper for $lt/$gt/$lte/$gte.
var makeInequality = function (cmpValueComparator) {
  return {
    compileElementSelector: function (operand) {
      // Arrays never compare false with non-arrays for any inequality.
      // XXX This was behavior we observed in pre-release MongoDB 2.5, but
      //     it seems to have been reverted.
      //     See https://jira.mongodb.org/browse/SERVER-11444
      if (isArray(operand)) {
        return function () {
          return false;
        };
      }

      // Special case: consider undefined and null the same (so true with
      // $gte/$lte).
      if (operand === undefined)
        operand = null;

      var operandType = LocalCollection._f._type(operand);

      return function (key) {
        return cmpValueComparator(key, operand);
      };
    }
  };
};

// Each element selector contains:
//  - compileElementSelector, a function with args:
//    - operand - the "right hand side" of the operator
//    - valueSelector - the "context" for the operator (so that $regex can find
//      $options)
//    - matcher - the Matcher this is going into (so that $elemMatch can compile
//      more things)
//    returning a function mapping a single value to bool.
//  - dontExpandLeafArrays, a bool which prevents expandArraysInBranches from
//    being called
//  - dontIncludeLeafArrays, a bool which causes an argument to be passed to
//    expandArraysInBranches if it is called
ELEMENT_OPERATORS = {
  $lt: makeInequality(function (key, operand) {
    return key + " < " + _prepareOperand(operand);
  }),
  $gt: makeInequality(function (key, operand) {
    return key + " > " + _prepareOperand(operand);
  }),
  $lte: makeInequality(function (key, operand) {
    return key + " <= " + _prepareOperand(operand);
  }),
  $gte: makeInequality(function (key, operand) {
    return key + " >= " + _prepareOperand(operand);
  }),
  $mod: {
    compileElementSelector: function (operand) {
      if (!(isArray(operand) && operand.length === 2
            && typeof(operand[0]) === 'number'
            && typeof(operand[1]) === 'number')) {
        throw Error("argument to $mod must be an array of two numbers");
      }
      // XXX could require to be ints or round or something
      var divisor = operand[0];
      var remainder = operand[1];
      return function (key) {
   	    return "mod("+key+ ", " + _prepareOperand(divisor)+") = "+_prepareOperand(remainder);
      };
    }
  },
  $in: {
	    compileElementSelector: function (operand) {
	      if (!isArray(operand))
	        throw Error("$in needs an array");

	      var elementMatchers = [];
	      _.each(operand, function (option) {
	        if (option instanceof RegExp)
		        throw Error("regexp inside $in is not supported yet");
	        else if (isOperatorObject(option))
	          throw Error("cannot nest $ under $in");
	        else
	          elementMatchers.push(option);
	      });

	      if(elementMatchers.length === 0) {
	          throw Error("no values applied to $in");    	  
	      }
	      return function (key) {
	    	  	var i = 0;
	    	    var ret = key + " IN (";
	    	    _.any(elementMatchers, function (e) {
	    	    	if(i > 0) {
	    	    		ret = ret + ", ";
	    	    	}
	    	    	ret = ret + _prepareOperand(e);
	    	    	i++;
	    	    	
	    	    	return false;
	    	    });
	    	    ret = ret + ")";
	    	    return ret;
	      };
	    }
	  },
  $nin: {
		    compileElementSelector: function (operand) {
		      if (!isArray(operand))
		        throw Error("$in needs an array");

		      var elementMatchers = [];
		      _.each(operand, function (option) {
		        if (option instanceof RegExp)
			        throw Error("regexp inside $nin is not supported yet");
		        else if (isOperatorObject(option))
		          throw Error("cannot nest $ under $in");
		        else
		          elementMatchers.push(option);
		      });

		      if(elementMatchers.length === 0) {
		          throw Error("no values applied to $in");    	  
		      }
		      return function (key) {
		    	  	var i = 0;
		    	    var ret = key + " NOT IN (";
		    	    _.any(elementMatchers, function (e) {
		    	    	if(i > 0) {
		    	    		ret = ret + ", ";
		    	    	}
		    	    	ret = ret + _prepareOperand(e);
		    	    	i++;
		    	    	
		    	    	return false;
		    	    });
		    	    ret = ret + ")";
		    	    return ret;
		      };
		    }
		  },
  $size: {      
    // {a: [[5, 5]]} must match {a: {$size: 1}} but not {a: {$size: 2}}, so we
    // don't want to consider the element [5,5] in the leaf array [[5,5]] as a
    // possible value.
    dontExpandLeafArrays: true,
    compileElementSelector: function (operand) {
      throw Error("$size operator is not supported yet");
    }
  },
  $type: {
    // {a: [5]} must not match {a: {$type: 4}} (4 means array), but it should
    // match {a: {$type: 1}} (1 means number), and {a: [[5]]} must match {$a:
    // {$type: 4}}. Thus, when we see a leaf array, we *should* expand it but
    // should *not* include it itself.
    dontIncludeLeafArrays: true,
    compileElementSelector: function (operand) {
      throw Error("$type operator is not supported yet");
    }
  },
  $regex: {
    compileElementSelector: function (operand, valueSelector) {
      if (!(typeof operand === 'string' || operand instanceof RegExp))
        throw Error("$regex has to be a string or RegExp");

      var regexp;
      if (valueSelector.$options !== undefined) {
        if (/[^gim]/.test(valueSelector.$options))
          throw new Error("Only the i, m, and g regexp options are supported");
      
        var regexSource = operand instanceof RegExp ? operand.source : operand;
        regexp = new RegExp(regexSource, valueSelector.$options).toString();
      } else if (operand instanceof RegExp) {
        regexp = operand.toString();
      } else {
        regexp = operand;
      }
      return regexpElementMatcher(regexp);
    }
  },
  $elemMatch: {
    dontExpandLeafArrays: true,
    compileElementSelector: function (operand, valueSelector, matcher) {
        throw Error("$elemMatch operator is not supported yet");
    }
  }
};

// NB: We are cheating and using this function to implement "AND" for both
// "document matchers" and "branched matchers". They both return result objects
// but the argument is different: for the former it's a whole doc, whereas for
// the latter it's an array of "branched values".
var operatorMatchers = function (subMatchers, operator) {
  if (subMatchers.length === 0)
    return "";
  if (subMatchers.length === 1)
    return subMatchers[0];

  	var i = 0;
    var ret = "";
    _.all(subMatchers, function (f) {
    	if(f && f !== "") {
        	if(i > 0) {
        		ret = ret + " "+operator+" ";
        	}
        	ret = ret + "(" + f + ")";
        	i++;
    	}
    	return true;
    });

    return ret;
};

var andDocumentMatchers = function(subMatchers) {return operatorMatchers(subMatchers, "AND");};
var orDocumentMatchers = function(subMatchers) {return operatorMatchers(subMatchers, "OR");};
var andBranchedMatchers = andDocumentMatchers;