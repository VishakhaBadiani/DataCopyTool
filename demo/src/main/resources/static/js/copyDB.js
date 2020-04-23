
var fAuthFlag=false;
var tAuthFlag=false;
var gfSName;
var gfPWD;
var gfDB;
var gtSName;
var gtPWD;
var gtDB;

$(document).ready(function(){
    console.log('document is loaded');
       
    
    $('#headingTwo').click(function(){
        $('#collapseTwo').toggle();
    });
    $('#headingOne').click(function(){
        $('#collapseOne').toggle();
    });
    $('#part').hide();
    $('#Qry').hide();

    $('#copyType').click(function(){
        var ctype=$('#copyType').val();
        if(ctype=='TC'){
            $('#part').hide();
            $('#Qry').hide();
        }
        if(ctype=='PC'){
            $('#part').show();
            $('#Qry').hide();
        }
        if(ctype=='CC'){
            $('#part').hide();
            $('#Qry').show();
        }
    });


    $('#fAuth').click(function(){

        var fSName= $('#fSName').val();
        var pwd=$('#fPWD').val();
        var fDBName=$('#fDBName').val();
        gfSName = fSName;
        gfPWD = pwd;
        gfDB = fDBName;

        if(validateF(fDBName,fSName,pwd)){
            
            console.log({usr:fSName,pass:pwd,dbn:fDBName});
            $.post('/authDB',{usr:fSName,pass:pwd,dbn:fDBName,DbType:"source"})
            .done(function(response){
                console.log(response);

                if (response == "false"){
                    //alert("Better luck next time!");
                    alert("Source DB: Better luck next time!");
                }
                else {
                    fAuthFlag= true;
                    if (fAuthFlag) {
                        console.log("line: 60 fAuthFlag is True");
                    }
                    alert("Welcome! You are authenticated to Source DB.");
                }
                console.log("Checking at line 64");
            if(fAuthFlag && tAuthFlag){
                console.log("Checking at line 66");
                $('#collapseOne').hide();
                $('#part').hide();
                $('#Qry').hide();
                loadTableList();
                $('#collapseTwo').show();
            }

            })
            .fail( function(xhr, textStatus, errorThrown) {
                alert("Sorce DB Connect Failed! "+xhr.responseText);
            });

            document.getElementById("fPWD").value='';
        }

    });

    $('#tAuth').click(function(){

        var tSName= $('#tSName').val();
        var pwd=$('#tPWD').val();
        var tDBName=$('#tDBName').val();
        gtSName = tSName;
        gtPWD = pwd;
        gtDB = tDBName;
        if(validateF(tDBName,tSName,pwd)){
            console.log({usr:tSName,pass:pwd,dbn:tDBName});
            $.post('/authDB',{usr:tSName,pass:pwd,dbn:tDBName,DbType:"target"})
            .done(function(response){
                console.log(response);

                if (response == "false"){
                    //alert("Better luck next time!");
                     alert("Better luck next time!");
                }
                else {
                    tAuthFlag= true;
                    if (fAuthFlag) {
                        console.log("line: 108 fAuthFlag is True");
                    }
                    if (tAuthFlag) {
                        console.log("line: 111 tAuthFlag is True");
                    }
                    alert("Welcome! You are authenticated to Target DB.");
                }
                console.log("Checking at line 109");
                if(fAuthFlag && tAuthFlag){
                    console.log("Checking at line 111");
                    $('#collapseOne').hide();
                    $('#part').hide();
                    $('#Qry').hide();
                    $('#collapseTwo').show();
                    loadTableList();
            }
            })
            .fail( function(xhr, textStatus, errorThrown) {
                alert("Target DB Connect Failed! "+xhr.responseText);
            });


            document.getElementById("tPWD").value='';

        }

    });


    $('#collapseTwo').show(function(){
        loadTableList();
    });

    $('#copyType').click(function(){
        loadPartition();
    });

    $('#tableName').click(function(){
        loadPartition();
    });

    $('#sub').click(function(){

            var tableName= $('#tableName').val();
            var copyType= $('#copyType').val();
            var Textarea= $('#Textarea').val();
            var Partition= $('#Partition').val();



            if(gfDB=='' || gfDB==undefined || gfSName=='' || gfSName==undefined|| gfPWD=='' || gfPWD==undefined){
                alert('Please Authenticate Source DB!');
                return false;
            }

            if(gtDB=='' || gtDB==undefined || gtSName=='' || gtSName==undefined || gtPWD=='' || gtPWD==undefined){
                alert('Please Authenticate Target DB!');
                return false;
            }

            if(tableName==''){
                alert('Please select table Name!');
                return false;
            }
            else if( copyType==''){
                alert('Please select Copy Type!');
                return false;
            }else if(copyType=='CC' && Textarea==''){
                alert('Please provide Query!');
                return false;

            }else if(copyType=='PC' && Partition==''){
                alert('Please select partition!');
                return false;

            }
            if(!confirm("Are you sure want to submit!")){
                return false;
            }
           var jobDetails=JSON.stringify({
                    'fromSchName': gfSName,
                    'fromPWD': gfPWD,
                    'fromDB': gfDB,
                    'toSchName': gtSName,
                    'toPWD': gtPWD,
                    'toDB': gtDB,
                    'tableName': tableName,
                     'copyType': copyType,
                     'textArea': Textarea,
                     'partition': Partition,
           });

             $.ajax({
              type: 'POST',
              url: '/copyData',
              contentType: 'application/json',
              dataType: 'json',
              data: jobDetails,

      });

      alert("Your Request has been Successfully Submitted. For details please check Dashboard!");
                 document.getElementById("mainForm").reset();
                 fAuthFlag=false;
                 tAuthFlag=false;
                 $('#collapseTwo').toggle();
                 $('#collapseOne').toggle();

    });




    $('#dashboard-tab').click(function(){
       var obj=$('#dashtable');

           var dashbody=$('#dashbody');
            dashbody.empty();
            $.ajax({
                type: 'GET',
                url: '/getAllData',
                success: function(result) {
                    console.log('success:',result);
                    $.each(result,function(i,item){
                    dashbody.append('<tr>');
                     dashbody.append('<td>'+item.jobId+'</td><td>'+item.fromDB+'</td>');
                     dashbody.append ('<td>'+item.fromSchName +'</td><td>'+item.toDB +'</td><td>'+item.toSchName +'</td>')
                     dashbody.append ('<td>'+item.tableName +'</td><td>'+item.copyType +'</td><td>'+item.createTime +'</td>');
                     dashbody.append ('<td>'+item.endTime +'</td><td>'+item.status +'</td>');
                     dashbody.append('</tr>');
                    });
                },
                error: function(result){
                    console.log('failure',result);
                    alert("System Error. Please contact admin");

                }
             });

        });



});

function validateF(fDBName,fSName,pwd){

    if(fDBName=='' ){
        alert('DB Name cannot be blank!');
        return false;
    }
    if(fSName==''){
        alert('Schema Name cannot be blank!');
        return false;
    }
    if(pwd==''){
        alert('Password cannot be blank!');
        return false;
    }
    return true;

}

function loadOptions(){
        console.log('fsname drop down');
        $('#collapseTwo').hide();
        $('#fSName').empty();
        $.ajax({
         type: 'GET',
         url: '/getSchName',
         //contentType: 'application/json',
         success : function(data){
             console.log(data);
            // var res = $.parseJSON(data);
            $('#fSName').append("<option value='' selected>--Select--</option>");
             $.each(data, function(index, value){
                 console.log(value);
                 $('#fSName').append("<option value='" + value + "'>"+value+"</option>");
             });
         },
         error: function(){alert("fSName: Option details not avaialble!");}
        }) ;

        console.log('tSName drop down');
        $('#tSName').empty();
        $.ajax({
         type: 'GET',
         url: '/getSchName',
         //contentType: 'application/json',
         success : function(data){
             console.log(data);
            // var res = $.parseJSON(data);
            $('#tSName').append("<option value='' selected>--Select--</option>");
             $.each(data, function(index, value){
                 console.log(value);
                 $('#tSName').append("<option value='" + value + "'>"+value+"</option>");
             });
         },
         error: function(){alert("tSName: Option details not avaialble!");}
        }) ;

        console.log('fDBName drop down');
        $('#fDBName').empty();
        $.ajax({
         type: 'GET',
         url: '/getSrcDBName',
         //contentType: 'application/json',
         success : function(data){
             console.log(data);
            // var res = $.parseJSON(data);
            $('#fDBName').append("<option value='' selected>--Select--</option>");
             $.each(data, function(index, value){
                 console.log(value);
                 $('#fDBName').append("<option value='" + value + "'>"+value+"</option>");
             });
         },
         error: function(){alert("fDBName: Option details not avaialble!");}
        }) ;

        console.log('tDBName drop down');
        $('#tDBName').empty();
        $.ajax({
         type: 'GET',
         url: '/getSrcDBName',
         //contentType: 'application/json',
         success : function(data){
             console.log(data);
            // var res = $.parseJSON(data);
            $('#tDBName').append("<option value='' selected>--Select--</option>");
             $.each(data, function(index, value){
                 console.log(value);
                 $('#tDBName').append("<option value='" + value + "'>"+value+"</option>");
             });
         },
         error: function(){alert("tDBName: Option details not avaialble!");}
        }) ;
return true;
}

function loadPartition(){
    if ($('#copyType').val() == 'PC'){
        console.log('Partition drop down');
        $('#Partition').empty();
        l_url = "/getPartName/" + $('#tableName').val();
        console.log("The partion for url : " + l_url);

        $.ajax({
        type: 'GET',
        url: l_url,
        //contentType: 'application/json',
        success : function(data){
            console.log(data);
            // var res = $.parseJSON(data);
            $('#Partition').append("<option value='' selected>--Select--</option>");
            $.each(data, function(index, value){
                console.log(value);
                $('#Partition').append("<option value='" + value + "'>"+value+"</option>");
            });
        },
        error: function(){alert("Partition: Option details not avaialble!");}
        }) ;
    return true;
    }
}

function loadTableList(){
        console.log('tableName drop down');
        $('#tableName').empty();
        if (gfSName != '' && gfSName != undefined){
            console.log('tableName inside if block');
                $.ajax({
                type: 'GET',
                url: '/getTabName',
                success : function(data){
                    console.log(data);

                    $('#tableName').append("<option value='' selected>--Select--</option>");
                    $.each(data, function(index, value){
                        console.log(value);
                        $('#tableName').append("<option value='" + value + "'>"+value+"</option>");
                    });
                },
                error: function(){alert("tableName: Option details not avaialble!");}
                }) ;
            }
    }
