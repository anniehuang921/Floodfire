$(document).ready(function(){
  $("#checkAll").click(function () {
      $(".check").prop('checked', $(this).prop('checked'));
  });
});


$(document).ready($(function () {
  $('#datetimepicker6').datetimepicker();
  $('#datetimepicker7').datetimepicker({
      useCurrent: false //Important! See issue #1075
      });
     $("#datetimepicker6").on("dp.change", function (e) {
      $('#datetimepicker7').data("DateTimePicker").minDate(e.date);
        });
        $("#datetimepicker7").on("dp.change", function (e) {
            $('#datetimepicker6').data("DateTimePicker").maxDate(e.date);
        });
    }));

$(function() {

        //
        // $('#instructorSelector').on('change', function() {
        //     $('#btn').show();
        // });
        $('.instructorSelector').hide();

        $('#btn').on('click', function() {
            $('.instructorSelector').show();
            return false;
        //     $('#instructorSelector').css('display', 'none');
        // }, function(){
        //   $('#instructorSelector').css('display', 'block');
        });

        // $('#btn').on('click', function() {
        //     var select_value = $('#instructorSelector').val();

            // use the value here

    });
