// code from 
// http://www.adp-gmbh.ch/web/js/hiding_column.html

if (document.getElementsByClassName == undefined) {
        document.getElementsByClassName = function(className)
        {
                var hasClassName = new RegExp("(?:^|\\s)" + className + "(?:$|\\s)");
                var allElements = document.getElementsByTagName("*");
                var results = [];

                var element;
                for (var i = 0; (element = allElements[i]) != null; i++) {
                        var elementClass = element.className;
                        if (elementClass && elementClass.indexOf(className) != -1 && hasClassName.test(elementClass))
                                results.push(element);
                }

                return results;
        }
}


function show_hide_data(myclass, do_show, reset_nodes) {

    var stl;
    if (do_show) stl = 'block'
    else         stl = 'none';

if (document.getElementsByClassName == undefined) {
	document.getElementsByClassName = function(className)
	{
		var hasClassName = new RegExp("(?:^|\\s)" + className + "(?:$|\\s)");
		var allElements = document.getElementsByTagName("*");
		var results = [];

		var element;
		for (var i = 0; (element = allElements[i]) != null; i++) {
			var elementClass = element.className;
			if (elementClass && elementClass.indexOf(className) != -1 && hasClassName.test(elementClass))
				results.push(element);
		}

		return results;
	}
}



    var elements  = document.getElementsByClassName(myclass);

    for (var el=0; el<elements.length;el++) {

        elements[el].style.display=stl;
    }
    if (reset_nodes) {
  // set/clear all checkboxes for individual jobs too
  var elements  = document.getElementsByClassName("job_indiv");

  for (var el=0; el<elements.length;el++) {
	
        elements[el].checked=do_show;
  }
    }



}


function show_hide_data_id(id, do_show) {

    var stl;
    if (do_show) stl = 'block'
    else         stl = 'none';




    var element  = document.getElementById(id);

//    alert(elements.length+id);
//    for (var el=0; el<elements.length;el++) {


//        alert("Setting element "+el+" to "+stl);
        element.style.display=stl;
//    }
  }

function toggle_job_blink(owner) {
	//alert("Displaying jobs for "+owner);

  var elements  = document.getElementsByClassName(owner);

  for (var el=0; el<elements.length;el++) {
	alert("x"+elements[el].style.color+"x");
	elements[el].style.textDecoration='blink';
	elements[el].style.backgroundColour='blue';
//	if (elements[el].visibility == '') {
//		elements[el].style.display='none';
//	} else {
//
//	elements[el].style. == 'block'
//	}


  }

}

function highlight(owner) {
	//alert("Displaying jobs for "+owner);

  var elements  = document.getElementsByClassName(owner);

  for (var el=0; el<elements.length;el++) {
//	alert("x"+elements[el].style.color+"x");
	elements[el].style.textDecoration='blink';
	elements[el].style.color='white';
	elements[el].style.backgroundColor='blue';
//	if (elements[el].visibility == '') {
//		elements[el].style.display='none';
//	} else {
//
//	elements[el].style. == 'block'
//	}


  }

}


function dehighlight(owner) {
	//alert("Displaying jobs for "+owner);

  var elements  = document.getElementsByClassName(owner);

  for (var el=0; el<elements.length;el++) {
//	alert("x"+elements[el].style.color+"x");
	elements[el].style.textDecoration='none';
	elements[el].style.backgroundColor='white';
	elements[el].style.color='black';
//	if (elements[el].visibility == '') {
//		elements[el].style.display='none';
//	} else {
//
//	elements[el].style. == 'block'
//	}


  }

}



function on_top(myclass, on_top) {

    var stl;
    if (on_top) stl = 'fixed'
    else         stl = 'static';

if (document.getElementsByClassName == undefined) {
        document.getElementsByClassName = function(className)
        {
                var hasClassName = new RegExp("(?:^|\\s)" + className + "(?:$|\\s)");
                var allElements = document.getElementsByTagName("*");
                var results = [];

                var element;
                for (var i = 0; (element = allElements[i]) != null; i++) {
                        var elementClass = element.className;
                        if (elementClass && elementClass.indexOf(className) != -1 && hasClassName.test(elementClass))
                                results.push(element);
                }

                return results;
        }
}

    var elements  = document.getElementsByClassName(myclass);
    //var aheight;

    for (var el=0; el<elements.length;el++) {
        elements[el].style.position=stl;
    //    aheight = elements[el].offsetHeight;
    }

    //var element = document.getElementById(container);
    //element.style.height = aheight;

}




/* scan through the widgets and read their state */
function synchronise_options (){

    var indiv_nodes  = document.getElementsByClassName("job_indiv");
    var details  = document.getElementsByName("show_details");
    var showdetails=details[0].checked;
    show_hide_data('jobdata', details[0].checked, false);

    for (var node=0; node<indiv_nodes.length;node++) {
	mynode=indiv_nodes[node];
        if (mynode.checked != details[0].checked) {
	        show_hide_data_id(mynode.name,mynode.checked);
		      
	}

       
	
    }


    /*   fixed_header*/
    details  = document.getElementsByName("fixed_header");
    showdetails=details[0].checked;
    on_top('summary_box',details[0].checked);


    /* refresh*/
    details  = document.getElementsByName("refresh");
    showdetails=details[0].checked;
    set_refresh(details[0].checked);


}
