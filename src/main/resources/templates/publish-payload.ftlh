<#--
 Copyright 2021 Kafdrop contributors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<#import "/spring.ftl" as spring />
<#import "lib/template.ftlh" as template>

<@template.header "Publish Payload" />

<#setting number_format="0">
<div class="container" id="topic-overview" style="padding: 20px;">
    <h2 style="text-align: center">Publish Payload</h2>
    <hr style="border-top: 2px solid #232323;" />
    <div id="create-form">
        <form action="<@spring.url '/publish'/>" method="POST">
            <div class="container">
                <div class="row">
                    <div class="col-8">
                        <label class="form-label">Topic Name</label>
                        <select name="name" class="form-control" id="inputGroupSelect06">
                            <#list topicNames! as topic>
                                <#if topicName??>
                                    <#if topicName==topic.name>
                                        <option value="${topicName}" selected>${topicName}</option>
                                        <#else>
                                            <option value="${topic.name}">${topic.name}</option>
                                    </#if>
                                    <#else>
                                        <option value="${topic.name}">${topic.name}</option>
                                </#if>

                            </#list>
                        </select>
                    </div>
                    <div class="col-4">
                        <button class="btn btn-success float-end" type="submit"
                            style="color: #eeeeee; font-size: 20px; font-weight: bold; width: 100px; height: 100px; border-radius: 50px; box-shadow: #232323 0px 0px 10px; border: solid 2px #eeeeee;">
                            Publish
                        </button>
                    </div>
                </div>
            </div>
            <hr style="border-top: 2px dotted #232323;" />
    </div>
    <div class="container">
        <div class="row">
            <div class="col">
                <input type="checkbox" class="form-check-input" name="publishOptionalHeaders" value="true"
                    id="publishOptionalHeaders" />
                <label class="form-label">Publish Optional Headers</label>
            </div>
        </div>
        <div class="row" id="OptionalHeaders">
            <div class="col">
                <textarea name="optionalHeaders" style="height: inherit;" class="form-control"
                    placeholder="Headers consist of key-value pairs separated by an equal sign (‘=’).Multiple key-value pairs are separated by commas (‘ , ’). For example:&#10;headerKey1=headerValue1,headerKey2=headerValue2"></textarea>
            </div>
        </div>
    </div>
    <hr style="border-top: 2px dotted #232323;" />
    <div class="container">
        <div class="row">
            <div class="col">
                <label class="form-label">Payload</label>
            </div>
        </div>
        <div class="row">
            <div class="col">

                <#if payloadText??>
                    <textarea id="payload" name="payload" class="form-control" content="${payloadText}"
                        style="resize: none; height: 500px; overflow: scroll;" required>${payloadText}</textarea>
                    <#else>
                        <textarea id="payload" name="payload" class="form-control"
                            style="resize: none; height: 500px; overflow: scroll;" required></textarea>
                </#if>
            </div>
            <div class="col">
                <div id="payloadJSONView"
                    style="border: solid 1px; border-radius: 5px; width: 100%; height: 500px; padding: 20px; overflow: scroll;">
                    This is JSON Preview Section.<br /> To view payload as JSON, put some content in payload
                    section.
                </div>
            </div>
        </div>
    </div>
    </form>
</div>
<div>
    <#if errorMessage??>
        <p>Error in publishing on topic ${topicName??}: ${errorMessage}</p>
        <#elseif published??>
            <#-- <p>Successfully published on topic <a href="<@spring.url '/topic/${topicName}'/>">${topicName}</a></p>-->
    </#if>
</div>

<@template.footer />

<script>
    $(document).ready(function () {

        $("div#OptionalHeaders").hide();

        if ($("input#publishOptionalHeaders").is(":checked")) {
            $("div#OptionalHeaders").show();
        }

        $("input#publishOptionalHeaders").bind("change", function () {
            $("div#OptionalHeaders").toggle();
        });
        $("textarea#payload").bind("keyup", function () {
            const jsonContent = this.value.trim();
            if (jsonContent.length > 0) {
                $("div#payloadJSONView").html(jsonContent);
                $("div#payloadJSONView").jsonFormatter({
                    tab: ' ',
                    quoteKeys: true,
                    collapsible: true,
                    hideOriginal: true
                });
            } else {
                defaultMessageLoader();
            }
        });
    });

    const defaultMessageLoader = function () {
        $("div#payloadJSONView").html("This is JSON Preview Section.<br/> To view payload as JSON, put some content in payload section.");
    }
</script>
