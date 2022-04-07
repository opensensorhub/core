/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.resource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.vast.unit.UnitParserUCUM;
import org.vast.util.TimeExtent;
import j2html.rendering.HtmlBuilder;
import j2html.rendering.IndentedHtml;
import j2html.tags.ContainerTag;
import j2html.tags.DomContent;
import j2html.tags.Tag;
import j2html.tags.UnescapedText;
import net.opengis.swe.v20.AllowedValues;
import net.opengis.swe.v20.BlockComponent;
import net.opengis.swe.v20.DataChoice;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import net.opengis.swe.v20.DataRecord;
import net.opengis.swe.v20.HasCodeSpace;
import net.opengis.swe.v20.HasConstraints;
import net.opengis.swe.v20.HasUom;
import net.opengis.swe.v20.RangeComponent;
import net.opengis.swe.v20.ScalarComponent;
import net.opengis.swe.v20.Vector;
import static j2html.TagCreator.*;


/**
 * <p>
 * Base class for all HTML resource formatter
 * </p>
 * 
 * @param <K> Resource Key
 * @param <V> Resource Object
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public abstract class ResourceBindingHtml<K, V> extends ResourceBinding<K, V>
{
    protected static final DomContent NBSP = new UnescapedText("&nbsp;");
    
    protected static final String[] CSS_CARD_TITLE = {"card-title", "text-primary"};
    protected static final String[] CSS_CARD_SUBTITLE = {"card-subtitle", "text-muted", "mb-2"};
    protected static final String[] CSS_LINK_BTN_CLASSES = {"me-2", "mb-2", "btn", "btn-sm", "btn-outline-primary"};
    protected static final String CSS_CARD_TEXT = "card-text";
    protected static final String CSS_BOLD = "fw-bold";
    
    protected Writer writer;
    protected HtmlBuilder<Writer> html;
    protected boolean isCollection;
    protected UnitParserUCUM uomParser;
    
    
    protected ResourceBindingHtml(RequestContext ctx, IdEncoder idEncoder) throws IOException
    {
        super(ctx, idEncoder);
        this.writer = new BufferedWriter(new OutputStreamWriter(ctx.getOutputStream()));
        this.html =  IndentedHtml.into(writer);
    }
    
    
    protected void writeHeader() throws IOException
    {
        html.appendStartTag(html().getTagName()).completeTag();
        
        var jsonQueryParams = new HashMap<>(ctx.getParameterMap());
        jsonQueryParams.remove("format"); // remove format in case it's set
        jsonQueryParams.put("f", new String[] {ResourceFormat.JSON.getMimeType()});
        
        // head
        head(
            title("OpenSensorHub - SensorWeb API"),
            meta().withCharset("UTF-8"),
            link()
                .withRel("stylesheet")
                .withHref("https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css")
                .attr("integrity", "sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3")
                .attr("crossorigin", ""),
            link()
                .withRel("stylesheet")
                .withHref("https://cdn.jsdelivr.net/npm/bootstrap-icons@1.8.1/font/bootstrap-icons.css"),
            script()
                .withSrc("https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js")
                .attr("integrity", "sha384-ka7Sk0Gln4gmtz2MlQnikT1wXgYsOg+OMhuP+IlRH9sENBO0LRn5q+8nbTov4+1p")
                .attr("crossorigin", ""),
            getExtraHeaderContent()
        )
        .render(html);
        
        // start body
        html.appendStartTag(body().getTagName()).completeTag();
        
        // nav bar
        div(
            nav(
                div(
                    /*img().withSrc("https://opensensorhub.files.wordpress.com/2017/08/opensensorhub-logo2.png")
                         .withHeight("60px")*/
                    getBreadCrumbs()
                ),
                span(
                    text("View as "),
                    a("JSON").withHref(ctx.getRequestUrlWithQuery(jsonQueryParams))
                )
            )
            .withClasses("navbar", "navbar-light", "navbar-collapse", "d-flex", "justify-content-between", "align-items-center")
        )
        .withClasses("container", "bg-light")
        .render(html);
        
        // content panel
        html.appendStartTag(div().getTagName())
            .appendAttribute("class", "container py-4")
            .completeTag();
    }
    
    
    protected DomContent getBreadCrumbs()
    {
        return each(ctx.getRequestPath(), item -> {
            return span(item + " / ");
        });
    }
    
    
    protected DomContent getExtraHeaderContent()
    {
        return null;
    }
    
    
    protected void writeFooter() throws IOException
    {
        html.appendEndTag(div().getTagName());
        html.appendEndTag(body().getTagName());
        html.appendEndTag(html().getTagName());
    }
    
    
    protected void renderCard(String title, DomContent... content) throws IOException
    {
        getCard(title, content).render(html);
    }
    
    
    protected void renderCard(Tag<?> title, DomContent... content) throws IOException
    {
        getCard(title, content).render(html);
    }
    
    
    protected ContainerTag<?> getCard(String title, DomContent... content)
    {
        return getCard(span(title), content);
    }
    
    
    protected ContainerTag<?> getCard(Tag<?> title, DomContent... content)
    {
        return div()
            .withClass("card mt-4")
            .with(
                div()
                .withClass("card-body")
                .with(
                    h5(title).withClasses(CSS_CARD_TITLE),
                    each(content)
                 )
            );
    }
    
    
    protected DomContent getPropertyHtml(DataComponent comp)
    {
        return div(
            span(getComponentLabel(comp))
                .attr("title", comp.getDescription()),
            iff(comp.getDefinition() != null,
                a("(" + comp.getDefinition() + ")")
                    .withClass("small")
                    .withHref(comp.getDefinition())
                    .withTarget("_blank")
            )
        ).withClass("card-text");
    }
    
    
    protected DomContent getComponentOneLineHtml(DataComponent comp)
    {
        DomContent value = null;
        
        if (comp instanceof ScalarComponent)
        {
            value = span(((DataComponent)comp).getData().getStringValue());
        }
        else if (comp instanceof RangeComponent)
        {
            value = span(((DataComponent)comp).getData().getStringValue(0) + " - " + 
                         ((DataComponent)comp).getData().getStringValue(1));
        }
        else
            return null;
        
        return div()
            .withClass(CSS_CARD_TEXT)
            .with(
                span(getComponentLabel(comp))
                    .withTitle(comp.getDescription())
                    .withClass(CSS_BOLD),
                span(": "),
                value,
                iff(comp instanceof HasUom, span(getUomString((HasUom)comp))),
                sup(a(" ")
                    .withClass("text-decoration-none bi-link")
                    .withHref(comp.getDefinition())
                    .withTitle(comp.getDefinition())
                    .withTarget("_blank"))
            );
    }
    
    
    protected DomContent getComponentHtml(DataComponent comp)
    {
        var content = div();
        
        if (comp.getDescription() != null)
        {
            content.with(
                span(comp.getDescription()).withClasses(CSS_CARD_SUBTITLE)
            );
        }
        
        content.with(div(
            span("Type").withClass(CSS_BOLD),
            span(": "),
            span(comp.getClass().getSimpleName().replace("Impl", ""))
        ));
        
        if (comp.getName() != null)
        {
            content.with(div(
                span("Field Name").withClass(CSS_BOLD),
                span(": "),
                span(comp.getName())
            ));
        }
        
        if (comp.getDefinition() != null)
        {
            content.with(div(
                span("Definition").withClass(CSS_BOLD),
                span(": "),
                a(comp.getDefinition())
                    .withHref(comp.getDefinition())
                    .withTarget("_blank")
            ));
        }
        
        if (comp instanceof HasUom && ((HasUom)comp).getUom() != null)
        {
            DomContent uom;
            var code = ((HasUom)comp).getUom().getCode();
            if (code != null)
            {
                uom = span(getUomString((HasUom)comp));
            }
            else
            {
                var href = ((HasUom)comp).getUom().getHref();
                uom = a(href)
                    .withHref(href)
                    .withTarget("_blank");
            }
            
            content.with(div(
                span("Unit of measure").withClass(CSS_BOLD),
                span(": "),
                uom
            ));
        }
        
        if (comp instanceof HasCodeSpace && ((HasCodeSpace)comp).getCodeSpace() != null)
        {
            var codeSpace = ((HasCodeSpace)comp).getCodeSpace();
            content.with(div(
                span("Codespace").withClass(CSS_BOLD),
                span(": "),
                span(codeSpace)
            ));
        }
        
        if (comp instanceof HasConstraints)
        {
            var constraint = ((HasConstraints<?>)comp).getConstraint();
            
            if (constraint instanceof AllowedValues)
            {
                var allowedVals = (AllowedValues)constraint;
                var values = "";
                
                if (allowedVals.getNumValues() > 0)
                {
                    for (double val: allowedVals.getValueList())
                        values += val + ", ";
                }
                
                else if (allowedVals.getNumIntervals() > 0)
                {
                    for (double[] range: allowedVals.getIntervalList())
                        values += range + ", ";
                }
                
                if (values.length() > 2)
                    values = values.substring(0, values.length()-2);
                
                content.with(div(
                    span("Allowed Values").withClass(CSS_BOLD),
                    span(": "),
                    span(values)
                ));
            }
        }
        
        // process sub-components
        
        if (comp instanceof DataRecord || comp instanceof Vector || comp instanceof DataChoice)
        {
            for (int i = 0; i < comp.getComponentCount(); i++)
            {
                var child = comp.getComponent(i);
                var html = getComponentHtml(child);
                content.with(html);
            }
        }
        else if (comp instanceof BlockComponent)
        {
            var html = getComponentHtml(((BlockComponent) comp).getElementType());
            content.with(html);
        }
        
        return getCard(getComponentLabel(comp), content).withClasses("card", "mt-3");
    }
    
    
    protected String getUomString(HasUom comp)
    {
        var code = comp.getUom().getCode();
        if (code == null)
            return "";
        
        if (uomParser == null)
            uomParser = new UnitParserUCUM();
        
        var symbol = uomParser
            .findUnit(code)
            .getPrintSymbol();
        
        return symbol == null || code.equals(symbol) ? code : code + " (" + symbol + ")";
    }
    
    
    protected DomContent getEncodingHtml(DataEncoding enc)
    {
        return div();
    }
    
    
    protected String getComponentLabel(DataComponent comp)
    {
        if (comp.getLabel() != null)
            return comp.getLabel();
        
        return getPrettyName(comp.getName());
    }
    
    
    protected String getPrettyName(String name)
    {
        StringBuilder buf = new StringBuilder(name.replace("_", " "));
        for (int i=0; i<buf.length()-1; i++)
        {
            char c = buf.charAt(i);
            
            if (i == 0 || buf.charAt(i-1) == ' ')
            {
                char newcar = Character.toUpperCase(c);
                buf.setCharAt(i, newcar);
            }
            
            else if (Character.isUpperCase(c) && Character.isLowerCase(buf.charAt(i-1)))
            {
                buf.insert(i, ' ');
                i++;
            }
        }
        
        return buf.toString();
    }
    
    
    protected Tag<?> getTimeExtentHtml(TimeExtent te, String textIfNull)
    {
        if (te == null)
            return span(textIfNull);
        
        return div(
            span(te.begin().truncatedTo(ChronoUnit.SECONDS).toString()), br(),
            span(te.end().truncatedTo(ChronoUnit.SECONDS).toString() + (te.endsNow() ? " (Now)" : ""))
        ).withClass("ps-4");
    }
    
    
    protected void writePagination(Collection<ResourceLink> pagingLinks) throws IOException
    {
        nav(
            ul(
                each(pagingLinks, link ->
                    li(
                        a().withHref(link.href)
                           .withText(link.rel)
                           .withClass("page-link")
                    )
                    .withClasses("page-item")
                )
            )
            .withClasses("pagination", "my-4")
        ).render(html);
    }
    
    
    @Override
    public void startCollection() throws IOException
    {
        isCollection = true;
        writeHeader();
    }
    
    
    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        writePagination(links);
        writeFooter();
        writer.flush();
    }
    
    
    @Override
    public V deserialize() throws IOException
    {
        throw ServiceErrors.unsupportedFormat("text/html");
    }
}