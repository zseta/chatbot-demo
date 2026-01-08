import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List
import logging
import os
from fastapi.staticfiles import StaticFiles

from .movie_rag.movie_rag import MovieRAG
from .movie_rag.models import Movie
from .movie_rag.llm_provider import LLMProvider

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Movie Recommendation API",
    description="Get movie recommendations using ScyllaDB Vector Search",
    version="1.0.0"
)
current_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(current_dir, "static")
templates_dir = os.path.join(current_dir, "templates")

# static folder
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Add templates
templates = Jinja2Templates(directory=templates_dir)

# Initialize recommender
try:
    movie_rag = MovieRAG()
    logger.info("MovieRecommender initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize MovieRecommender: {e}")
    movie_rag = None

# Request/Response models
class RecommendationRequest(BaseModel):
    query: str
    top_k: int = 5

class RecommendationResponse(BaseModel):
    movies: List[Movie]
    query: str
    total_results: int
    

@app.post("/recommend", response_model=RecommendationResponse)
async def post_recommendations(request: RecommendationRequest):
    """Get movie recommendations via POST request"""
    
    if movie_rag is None:
        raise HTTPException(
            status_code=503,
            detail="Movie recommender service is not available"
        )
    
    try:
        logger.info(f"Getting recommendations for query: '{request.query}', top_k: {request.top_k}")
        movies = movie_rag.similar_movies(request.query, request.top_k)
        
        logger.info(f"Found {len(movies)} recommendations")
        
        return RecommendationResponse(
            movies=movies,
            query=request.query,
            total_results=len(movies)
        )
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get recommendations: {str(e)}"
        )


@app.get("/start-sse", response_class=HTMLResponse)
async def generate_story(request: Request, query, top_k):
    context = {"request": request,
               "query_string": request.url.query,
               "query": query,
               "top_k": top_k}
    print(query, top_k, "*********")
    return templates.TemplateResponse("partials/bot_message.html", context)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the main chat HTML page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/generate-story/stream")
async def generate_story_stream(request: Request, query: str, top_k: int):
    """Turn any movie plot into a Scylla story"""

    if movie_rag is None:
        raise HTTPException(
            status_code=503,
            detail="Service is not available"
        )
    
    llm_context_prompt = """ 
    Max 100 words. Rewrite the following movie plot as if it were a story 
    about a low-latency database named ScyllaDB. 
    Treat ScyllaDB as the protagonist. Keep the spirit and structure of
    the movie, but make it fit the database world.
    Don't mention any other specific databases by name. 
    The plot: {plot}"""
    
    try:
        llm_provider = LLMProvider()
        movies = movie_rag.similar_movies(query, top_k)
        movie = movies[0]
        rag_movie_plot = movie.plot
        context_prompt = llm_context_prompt.format(plot=rag_movie_plot)
        
        async def stream_generator():
            # Send movie data first
            movie_data = {
                "title": movie.title,
                "poster_url": movie.poster_url,
                "plot": movie.plot
            }
            yield f"event: movie_data\ndata: {json.dumps(movie_data)}\n\n"
            
            # Stream content chunks
            for chunk in llm_provider.generate_response_stream(
                "You are a chatbot, follow instructions", 
                context_prompt
            ):
                if chunk and chunk.strip():
                    yield f"event: content\ndata: {chunk}\n\n"
            
            # Send done event
            yield f"event: done\ndata: {json.dumps({'status': 'complete'})}\n\n"
        
        return StreamingResponse(
            stream_generator(), 
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"
            }
        )
    
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Run the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=3000,
        reload=True
    )
